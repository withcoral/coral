import { expect, test } from 'vitest'
import type { Page } from 'playwright'
import { createServer } from 'node:http'
import { managementClients } from '../../../../support/management.ts'
import { ProviderAccountScope } from '../../../../generated/coral/management/v1/account_pb.ts'
import { withOwnerSession } from '../../../../support/actors.ts'
import { listen, oauthMcpServer, openApiDocument } from '../../../../support/provider-fixtures.ts'
import {
  acceptOperations,
  addProviderFromFile,
  chooseAccount,
  chooseFile,
  confirmConfiguration,
  createWorkspace,
  openCustomSource,
  openOperation,
  openProviderSettings,
  operationGroup,
  programRunner,
  discardSettingsChange,
  saveConfirmedSettingsChange,
  setProviderEnabled,
  signInToReadOperations,
  waitForProviderStatus,
  waitForPublication,
} from '../../../../support/providers.ts'

// Every change returns to the provider's settings, where it opens.
const saved = (page: Page) => page.waitForURL((url) => url.pathname.endsWith('/settings'))
const operationLink = (page: Page, name: string) =>
  page
    .getByRole('table', { name: 'Operations', exact: true })
    .getByRole('link', { name, exact: true })
const save = async (page: Page) => {
  expect(new URL(page.url()).searchParams.has('operationsAccepted')).toBe(false)
  await page.getByRole('button', { name: 'Save', exact: true }).click()
  await saved(page)
}

test('Change account settings without reading a source or editing configuration', async (context) => {
  await withOwnerSession(context, async ({ page, step, actor }) => {
    const workspace = await createWorkspace(
      actor.accessToken,
      `Account setting ${crypto.randomUUID()}`,
    )
    const providerPath = await addProviderFromFile(
      page,
      workspace,
      openApiDocument('Records', { listRecords: {} }, { security: 'bearer' }),
      'records',
      { account: 'shared', secret: 'fixture-account-setting' },
    )
    await step('Choose the account setting directly from the provider', async () => {
      await openProviderSettings(page)
      await page.getByRole('button', { name: 'Change account setting', exact: true }).click()
      expect(await page.getByRole('button', { name: 'Save', exact: true }).isDisabled()).toBe(true)
      await page.getByRole('radio', { name: 'Use individual accounts', exact: true }).check()
      expect(
        await page.getByRole('textbox', { name: /^(OpenAPI URL|MCP server URL)$/ }).count(),
      ).toBe(0)
      expect(await page.getByLabel('Display name', { exact: true }).count()).toBe(0)
      await saveConfirmedSettingsChange(page)
      expect(new URL(page.url()).pathname).toBe(`${providerPath}/settings`)
      await page
        .getByText(
          'Each person connects their own account. Workspace owners connect accounts for agents.',
          { exact: true },
        )
        .waitFor()
    })
    await step('Ask before discarding an unsaved edit', async () => {
      await page.getByRole('button', { name: 'Edit configuration', exact: true }).click()
      await page.getByLabel('Display name', { exact: true }).fill('Unsaved records')
      const discard = page.getByRole('dialog', { name: 'Discard your changes?' })
      await page.getByRole('link', { name: 'Providers', exact: true }).first().click()
      await discard.getByRole('button', { name: 'Keep editing', exact: true }).click()
      expect(await page.getByLabel('Display name', { exact: true }).inputValue()).toBe(
        'Unsaved records',
      )
      await discardSettingsChange(page)
      expect(new URL(page.url()).pathname).toBe(`${providerPath}/settings`)
    })
    await step('Edit configuration without fetching a source', async () => {
      await page.getByRole('button', { name: 'Edit configuration', exact: true }).click()
      expect(await page.getByLabel('Display name', { exact: true }).inputValue()).toBe('Records')
      await page.getByLabel('Display name', { exact: true }).fill('Renamed records')
      expect(
        await page.getByRole('textbox', { name: /^(OpenAPI URL|MCP server URL)$/ }).count(),
      ).toBe(0)
      await page.getByRole('button', { name: 'Save', exact: true }).click()
      await saved(page)
      expect(new URL(page.url()).pathname).toBe(`${providerPath}/settings`)
      await page.getByRole('heading', { level: 2, name: 'Renamed records', exact: true }).waitFor()
    })
  })
})

test('Refresh keeps a saved document URL through configuration edits and clears it after an upload', async (context) => {
  let reads = 0
  let holdRead = false
  let releaseRead: (() => void) | undefined
  context.onTestFinished(() => releaseRead?.())
  let operations: Record<string, object> = { listRecords: {} }
  const source = await listen(
    context,
    createServer((request, response) => {
      if (request.method !== 'GET') {
        response.writeHead(404).end()
        return
      }
      reads++
      const send = () =>
        response
          .writeHead(200, { 'content-type': 'application/json' })
          .end(openApiDocument('Records', operations))
      if (holdRead)
        releaseRead = () => {
          holdRead = false
          releaseRead = undefined
          send()
        }
      else send()
    }),
  )
  await withOwnerSession(context, async ({ page, step, actor }) => {
    const workspace = await createWorkspace(actor.accessToken, `Refresh URL ${crypto.randomUUID()}`)
    await page.goto(`/workspaces/${workspace}/providers/new`)
    await openCustomSource(page)
    await page.getByLabel('OpenAPI URL', { exact: true }).fill(source.origin)
    await page.getByRole('button', { name: 'Read', exact: true }).click()
    await confirmConfiguration(page, 'No authentication')
    await acceptOperations(page)
    await page.getByRole('button', { name: 'Add provider', exact: true }).click()
    await waitForPublication(page)
    const providerPath = new URL(page.url()).pathname
    const initialReads = reads
    await step('Edit without refetching, then refresh the saved URL', async () => {
      await openProviderSettings(page)
      await page.getByRole('button', { name: 'Edit configuration', exact: true }).click()
      await page.getByLabel('Display name', { exact: true }).fill('Renamed records')
      await page.getByRole('button', { name: 'Save', exact: true }).click()
      await saved(page)
      expect(reads).toBe(initialReads)
      operations = { listRecords: {}, newReport: {} }
      await page.getByRole('link', { name: 'Check for updates', exact: true }).click()
      // The source shows as settings show it, redacted and normalised.
      await page.getByText(`${source.origin}/`, { exact: true }).waitFor()
      expect(reads).toBe(initialReads)
      expect(await page.getByRole('heading', { name: 'Changes', exact: true }).count()).toBe(0)
      expect(await page.getByRole('heading', { name: 'Review', exact: true }).count()).toBe(0)
      holdRead = true
      await page.getByRole('button', { name: 'Read operations', exact: true }).click()
      await page.getByRole('status').getByText('Reading operations…', { exact: true }).waitFor()
      // Nothing to save until the source is read.
      expect(await page.getByRole('button', { name: 'Save', exact: true }).isDisabled()).toBe(true)
      await expect.poll(() => typeof releaseRead).toBe('function')
      releaseRead!()
      // What the read adds opens at the top.
      await operationGroup(page, 'New').getByText('newReport', { exact: true }).waitFor()
      expect(await page.getByRole('textbox', { name: 'Display name', exact: true }).count()).toBe(0)
      expect(await page.getByRole('group', { name: 'Sign-in', exact: true }).count()).toBe(0)
      await save(page)
      expect(reads).toBe(initialReads + 1)
      await page.goto(providerPath)
      await operationLink(page, 'newReport').waitFor()
    })
    await step('An uploaded replacement no longer offers the old URL', async () => {
      await openProviderSettings(page)
      await page.getByRole('link', { name: 'Replace source', exact: true }).click()
      await chooseFile(page, 'replacement.json', openApiDocument('Renamed records', operations))
      await page.getByRole('button', { name: 'Read', exact: true }).click()
      await page.getByRole('button', { name: 'Save', exact: true }).waitFor()
      expect(await page.getByRole('textbox', { name: 'Display name', exact: true }).count()).toBe(0)
      await save(page)
      expect(reads).toBe(initialReads + 1)
      expect(new URL(page.url()).pathname).toBe(`${providerPath}/settings`)
      await page.getByRole('link', { name: 'Check for updates', exact: true }).click()
      await page.getByRole('link', { name: 'Replace document', exact: true }).waitFor()
      expect(await page.getByRole('button', { name: 'Read operations', exact: true }).count()).toBe(
        0,
      )
    })
  })
})

test('Start a canceled settings change again against the provider as it is now', async (context) => {
  // A source that counts its reads, so each Start again shows which source it read.
  const serve = async () => {
    const counter = { reads: 0, origin: '' }
    const fixture = await listen(
      context,
      createServer((request, response) => {
        if (request.method !== 'GET') {
          response.writeHead(404).end()
          return
        }
        counter.reads++
        response
          .writeHead(200, { 'content-type': 'application/json' })
          .end(openApiDocument('Records', { listRecords: {} }))
      }),
    )
    counter.origin = fixture.origin
    return counter
  }
  const original = await serve()
  const replacement = await serve()
  await withOwnerSession(context, async ({ page, step, actor }) => {
    const workspace = await createWorkspace(actor.accessToken, `Start again ${crypto.randomUUID()}`)
    await page.goto(`/workspaces/${workspace}/providers/new`)
    await openCustomSource(page)
    await page.getByLabel('OpenAPI URL', { exact: true }).fill(original.origin)
    await page.getByRole('button', { name: 'Read', exact: true }).click()
    await confirmConfiguration(page, 'No authentication')
    await acceptOperations(page)
    await page.getByRole('button', { name: 'Add provider', exact: true }).click()
    await waitForPublication(page)
    const providerPath = new URL(page.url()).pathname
    const setupId = () => new URL(page.url()).searchParams.get('setup')
    // Back returns from settings to the setup that Cancel ended.
    const returnToEnded = async () => {
      await page.goBack()
      await page.getByText('This setup is canceled', { exact: true }).waitFor()
    }
    await step('Start a canceled edit again after the provider changed', async () => {
      await openProviderSettings(page)
      await page.getByRole('button', { name: 'Edit configuration', exact: true }).click()
      await page.getByLabel('Display name', { exact: true }).waitFor()
      const ended = setupId()
      expect(ended).toBeTruthy()
      // Cancel with no changes leaves at once.
      await page.getByRole('button', { name: 'Cancel', exact: true }).click()
      await saved(page)
      // Another change moves the provider on. The ended setup's address keeps the
      // version it began with; the new setup must start from the version now.
      await setProviderEnabled(actor.accessToken, providerPath, false)
      await setProviderEnabled(actor.accessToken, providerPath, true)
      await returnToEnded()
      await page.getByRole('button', { name: 'Start again', exact: true }).click()
      await page.getByLabel('Display name', { exact: true }).fill('Renamed records')
      expect(setupId()).not.toBe(ended)
      await page.getByRole('button', { name: 'Save', exact: true }).click()
      await saved(page)
      await page.getByRole('heading', { level: 2, name: 'Renamed records', exact: true }).waitFor()
    })
    await step('Start a canceled replacement again from the replacement URL', async () => {
      await page.getByRole('link', { name: 'Replace source', exact: true }).click()
      await page.getByLabel('OpenAPI URL', { exact: true }).fill(replacement.origin)
      await page.getByRole('button', { name: 'Read', exact: true }).click()
      await page.getByRole('button', { name: 'Save', exact: true }).waitFor()
      expect(replacement.reads).toBe(1)
      const ended = setupId()
      expect(ended).toBeTruthy()
      // A read source is an unsaved change, so Cancel asks first.
      await discardSettingsChange(page)
      await returnToEnded()
      const originalReads = original.reads
      await page.getByRole('button', { name: 'Start again', exact: true }).click()
      await page.getByRole('button', { name: 'Save', exact: true }).waitFor()
      expect(setupId()).not.toBe(ended)
      expect(replacement.reads).toBe(2)
      expect(original.reads).toBe(originalReads)
      await save(page)
      await page.getByRole('link', { name: 'Check for updates', exact: true }).click()
      await page.getByText(`${replacement.origin}/`, { exact: true }).waitFor()
    })
  })
})

test('Recover a different-account sign-in and review configuration-triggered rediscovery', async (context) => {
  const token = 'fixture-update-recovery'
  const upstream = await oauthMcpServer(context, token)
  await withOwnerSession(context, async ({ page, step, actor }) => {
    const api = managementClients(actor.accessToken)
    const workspace = await createWorkspace(actor.accessToken, `Recovery ${crypto.randomUUID()}`)
    await page.goto(`/workspaces/${workspace}/providers/new`)
    await openCustomSource(page)
    await page.getByLabel('MCP server URL', { exact: true }).fill(upstream.mcpUrl)
    await page.getByRole('button', { name: 'Connect', exact: true }).click()
    await confirmConfiguration(page)
    await signInToReadOperations(page)
    await page.getByRole('link', { name: 'Approve fixture access', exact: true }).click()
    await acceptOperations(page)
    await chooseAccount(page, 'shared')
    await page.getByRole('button', { name: 'Add provider', exact: true }).click()
    await waitForPublication(page)
    const providerPath = new URL(page.url()).pathname
    const inspect = () =>
      api.accounts.getProviderAccount({
        parent: `workspaces/${workspace}`,
        provider: providerPath.slice(1),
        scope: ProviderAccountScope.SHARED,
      })
    const original = await inspect()
    await step(
      'Resume replacement sign-in after leaving consent, then recover its failure',
      async () => {
        await openProviderSettings(page)
        await page.getByRole('link', { name: 'Check for updates', exact: true }).click()
        await page.getByRole('button', { name: 'Read operations', exact: true }).click()
        await page.getByRole('button', { name: /^Sign in/ }).click()
        await page.getByRole('link', { name: 'Approve fixture access', exact: true }).click()
        await page.getByRole('radio', { name: 'Connect another account', exact: true }).check()
        // A new shared account is connected from the share question, as in Add provider.
        expect(await page.getByRole('button', { name: 'Save', exact: true }).count()).toBe(0)
        await page.getByRole('button', { name: 'Connect the shared account', exact: true }).click()
        await page
          .getByRole('dialog')
          .getByRole('button', { name: 'Sign in and share the account', exact: true })
          .click()
        await page.getByRole('link', { name: 'Approve fixture access', exact: true }).waitFor()
        await page.goBack()
        await page.reload()
        // The sign-in in progress survives the reload.
        await page.getByRole('button', { name: 'Continue sign-in', exact: true }).click()
        upstream.knobs.tokenError = 'access_denied'
        await page.getByRole('link', { name: 'Approve fixture access', exact: true }).click()
        // The failed sign-in stays in view, ready to try again.
        await page
          .getByRole('button', { name: 'Connect the shared account', exact: true })
          .waitFor()
        await page.getByRole('radio', { name: 'Keep the current account', exact: true }).check()
        // Keeping the account needs no consent, so Save publishes at once.
        await save(page)
        expect(await inspect()).toEqual(original)
      },
    )
    await step('Review the freshly read catalog after editing MCP authentication', async () => {
      upstream.knobs.tokenError = undefined
      await openProviderSettings(page)
      await page.getByRole('button', { name: 'Edit configuration', exact: true }).click()
      await confirmConfiguration(page, 'Bearer token', 'Save')
      await page.getByLabel('Token', { exact: true }).fill(token)
      await page.getByRole('button', { name: 'List operations', exact: true }).click()
      await page.getByRole('radio', { name: 'Use this account', exact: true }).check()
      await page.getByRole('button', { name: 'Save', exact: true }).click()
      const confirm = page.getByRole('dialog', { name: 'Save changes?' })
      await confirm
        .getByText(
          'This change replaces the shared account authorization. Pending calls cannot switch accounts.',
          { exact: true },
        )
        .waitFor()
      await confirm.getByRole('button', { name: 'Save', exact: true }).click()
      await saved(page)
    })
  })
})

test('Refresh an uploaded document with unreadable and stale-change protection', async (context) => {
  await withOwnerSession(context, async ({ page, target, step, actor }) => {
    const workspace = await createWorkspace(actor.accessToken, `Update ${crypto.randomUUID()}`)
    const providerPath = await addProviderFromFile(
      page,
      workspace,
      openApiDocument('Records', { listRecords: {}, oldReport: {} }),
      'records',
    )
    const upload = async (document: Buffer) => {
      await page.goto(`${providerPath}/update`)
      await chooseFile(page, 'records.json', document)
      await page.getByRole('button', { name: 'Read', exact: true }).click()
    }
    await step('A new document adds and removes operations', async () => {
      await upload(openApiDocument('Records', { listRecords: {}, newReport: {} }))
      await operationGroup(page, 'New').getByText('newReport', { exact: true }).waitFor()
      await operationGroup(page, 'Removed').getByText('oldReport', { exact: true }).waitFor()
      // Nothing breaks access, so Save asks nothing.
      expect(await page.getByRole('checkbox').count()).toBe(0)
      await save(page)
      expect(new URL(page.url()).pathname).toBe(`${providerPath}/settings`)
      await page.goto(providerPath)
      await operationLink(page, 'newReport').waitFor()
      expect(await operationLink(page, 'oldReport').count()).toBe(0)
      // The operation's full name keeps the namespace.
      const drawer = await openOperation(page, 'newReport')
      await drawer.getByText('coral.providers.records.newReport', { exact: true }).waitFor()
      await page.keyboard.press('Escape')
      await drawer.waitFor({ state: 'hidden' })
      const run = programRunner(page, target, workspace)
      expect(await run('return await coral.providers.records.oldReport({});')).not.toContain(
        'succeeded',
      )
      expect(await run('return typeof coral.providers.records.newReport;')).toContain('function')
    })
    await step('An unreadable document stops before configuration', async () => {
      await upload(
        Buffer.from(
          JSON.stringify({
            openapi: '3.1.0',
            info: { title: 'Records', version: '1' },
            servers: [{ url: 'http://127.0.0.1:1' }],
            paths: {
              '/a': { get: { operationId: 'same', responses: { '200': { description: 'ok' } } } },
              '/b': { get: { operationId: 'same', responses: { '200': { description: 'ok' } } } },
            },
          }),
        ),
      )
      const error = page.getByRole('alert').filter({ hasText: 'Cannot read this document' })
      await error.getByText('Error code: duplicate_operation_key', { exact: false }).waitFor()
      expect(await page.getByLabel('Display name', { exact: true }).count()).toBe(0)
      // An upload is not kept, so you choose the source again in settings.
      await error.getByRole('link', { name: 'Choose a source', exact: true }).click()
      await page.waitForURL((url) => url.pathname === `${providerPath}/settings`)
      await page.getByRole('link', { name: 'Replace source', exact: true }).click()
      await chooseFile(
        page,
        'corrected.json',
        openApiDocument('Records', { listRecords: {}, newReport: {} }),
      )
      await page.getByRole('button', { name: 'Read', exact: true }).click()
      await save(page)
    })
    await step('A change in another tab invalidates the reviewed request', async () => {
      await upload(openApiDocument('Records', { listRecords: {} }))
      await page.getByRole('button', { name: 'Save', exact: true }).waitFor()
      // Another change moves the provider on, as a second tab or operator would.
      await setProviderEnabled(actor.accessToken, providerPath, false)
      await setProviderEnabled(actor.accessToken, providerPath, true)
      await page.getByRole('button', { name: 'Save', exact: true }).click()
      await page
        .getByText(
          'This provider changed. Reload the page. Review the changes before you try again.',
          { exact: true },
        )
        .waitFor()
      await page.goto(providerPath)
      await waitForProviderStatus(page, 'Records', 'Ready', { available: 2, total: 2 })
    })
  })
})
