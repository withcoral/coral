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
  programRunner,
  setProviderEnabled,
  signInToReadOperations,
  waitForProviderStatus,
  waitForPublication,
} from '../../../../support/providers.ts'

const applied = (page: Page) => waitForPublication(page)
const operationLink = (page: Page, name: string) =>
  page
    .getByRole('table', { name: 'Operations', exact: true })
    .getByRole('link', { name, exact: true })
const apply = async (page: Page) => {
  expect(new URL(page.url()).searchParams.has('operationsAccepted')).toBe(false)
  expect(await page.getByRole('link', { name: 'Next', exact: true }).count()).toBe(0)
  await page.getByRole('button', { name: 'Apply changes', exact: true }).click()
  await applied(page)
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
      await page.getByRole('radio', { name: 'Individual accounts', exact: true }).check()
      expect(
        await page.getByRole('textbox', { name: /^(OpenAPI URL|MCP server URL)$/ }).count(),
      ).toBe(0)
      expect(await page.getByLabel('Display name', { exact: true }).count()).toBe(0)
      await page.getByRole('button', { name: 'Review changes', exact: true }).click()
      await page.getByRole('heading', { name: 'Review changes', exact: true }).waitFor()
      expect(
        await page.getByRole('button', { name: 'Apply changes', exact: true }).isDisabled(),
      ).toBe(true)
      await page
        .getByRole('checkbox', { name: 'Remove existing shared account credentials', exact: true })
        .check()
      await apply(page)
      expect(new URL(page.url()).pathname).toBe(providerPath)
      await openProviderSettings(page)
      await page.getByText('Each caller uses their own account.', { exact: true }).waitFor()
    })
    await step('Edit configuration without fetching a source', async () => {
      await page.getByRole('button', { name: 'Edit configuration', exact: true }).click()
      await page.getByLabel('Display name', { exact: true }).fill('Renamed records')
      expect(
        await page.getByRole('textbox', { name: /^(OpenAPI URL|MCP server URL)$/ }).count(),
      ).toBe(0)
      await page.getByRole('button', { name: 'Next', exact: true }).click()
      await page.getByText('Keep existing accounts.', { exact: true }).waitFor()
      await apply(page)
      await page.getByRole('rowgroup', { name: 'Renamed records', exact: true }).waitFor()
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
      await page.getByRole('button', { name: 'Next', exact: true }).click()
      await apply(page)
      expect(reads).toBe(initialReads)
      operations = { listRecords: {}, newReport: {} }
      await openProviderSettings(page)
      await page.getByRole('link', { name: 'Refresh operations', exact: true }).click()
      await page.getByText(source.origin, { exact: true }).waitFor()
      expect(reads).toBe(initialReads)
      expect(await page.getByRole('heading', { name: 'Changes', exact: true }).count()).toBe(0)
      expect(await page.getByRole('heading', { name: 'Review', exact: true }).count()).toBe(0)
      holdRead = true
      await page.getByRole('button', { name: 'Read operations', exact: true }).click()
      await page
        .getByRole('status')
        .getByText('Reading operations. Large catalogs can take a moment.', {
          exact: true,
        })
        .waitFor()
      expect(
        await page.getByRole('button', { name: 'Reading operations…', exact: true }).isDisabled(),
      ).toBe(true)
      await expect.poll(() => typeof releaseRead).toBe('function')
      releaseRead!()
      await page.getByRole('button', { name: 'Apply changes', exact: true }).waitFor()
      expect(await page.getByRole('textbox', { name: 'Display name', exact: true }).count()).toBe(0)
      expect(await page.getByRole('group', { name: 'Sign-in', exact: true }).count()).toBe(0)
      await apply(page)
      expect(reads).toBe(initialReads + 1)
      await operationLink(page, 'newReport').waitFor()
    })
    await step('An uploaded replacement no longer offers the old URL', async () => {
      await openProviderSettings(page)
      await page.getByRole('link', { name: 'Replace source', exact: true }).click()
      await chooseFile(page, 'replacement.json', openApiDocument('Renamed records', operations))
      await page.getByRole('button', { name: 'Read', exact: true }).click()
      await page.getByRole('button', { name: 'Apply changes', exact: true }).waitFor()
      expect(await page.getByRole('textbox', { name: 'Display name', exact: true }).count()).toBe(0)
      await apply(page)
      expect(reads).toBe(initialReads + 1)
      expect(new URL(page.url()).pathname).toBe(providerPath)
      await openProviderSettings(page)
      await page.getByRole('link', { name: 'Refresh operations', exact: true }).click()
      await page.getByRole('link', { name: 'Replace document', exact: true }).waitFor()
      expect(await page.getByRole('button', { name: 'Read operations', exact: true }).count()).toBe(
        0,
      )
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
        await page.getByRole('link', { name: 'Refresh operations', exact: true }).click()
        await page.getByRole('button', { name: 'Read operations', exact: true }).click()
        await page.getByRole('button', { name: /^Sign in/ }).click()
        await page.getByRole('link', { name: 'Approve fixture access', exact: true }).click()
        await page.getByRole('radio', { name: 'Connect a different account', exact: true }).check()
        await page.getByRole('button', { name: 'Sign in again', exact: true }).click()
        await page.getByRole('link', { name: 'Approve fixture access', exact: true }).waitFor()
        await page.goBack()
        await page.reload()
        await page.getByRole('button', { name: 'Continue sign-in', exact: true }).click()
        upstream.knobs.tokenError = 'access_denied'
        await page.getByRole('link', { name: 'Approve fixture access', exact: true }).click()
        await page.getByRole('button', { name: 'Sign in again', exact: true }).waitFor()
        await page
          .getByRole('radio', { name: 'Keep the existing shared account', exact: true })
          .check()
        await page.getByRole('button', { name: 'Review changes', exact: true }).click()
        await page.getByText('Keep existing accounts.', { exact: true }).waitFor()
        await apply(page)
        expect(await inspect()).toEqual(original)
      },
    )
    await step('Review the freshly read catalog after editing MCP authentication', async () => {
      upstream.knobs.tokenError = undefined
      await openProviderSettings(page)
      await page.getByRole('button', { name: 'Edit configuration', exact: true }).click()
      await confirmConfiguration(page, 'Bearer token')
      await page.getByLabel('Token', { exact: true }).fill(token)
      await page.getByRole('button', { name: 'List operations', exact: true }).click()
      await page
        .getByRole('radio', { name: 'Use this sign-in as the shared account', exact: true })
        .check()
      await page.getByRole('button', { name: 'Review changes', exact: true }).click()
      await page.getByText('Use the reviewed catalog', { exact: true }).waitFor()
      expect(await page.getByText('Keep the current catalog', { exact: true }).count()).toBe(0)
      await page
        .getByRole('checkbox', { name: 'Replace the shared account authorization', exact: true })
        .check()
      await apply(page)
    })
  })
})

test('Refresh an uploaded document with failure, replay and stale-change protection', async (context) => {
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
      await page.getByRole('button', { name: 'Apply changes', exact: true }).waitFor()
      expect(await page.getByRole('checkbox').count()).toBe(0)
      await page.getByText('The namespace and policy stay the same.', { exact: true }).waitFor()
      await apply(page)
      expect(new URL(page.url()).pathname).toBe(providerPath)
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
    let failedUrl = ''
    await step('Failure preserves the catalog and replay returns that failure', async () => {
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
      await page.getByRole('button', { name: 'Remove this saved selection', exact: true }).click()
      await confirmConfiguration(page)
      failedUrl = new URL(
        (await page.locator('form:has(input[value="publish"])').getAttribute('action'))!,
        page.url(),
      ).toString()
      await page.getByRole('button', { name: 'Apply changes', exact: true }).click()
      await page.getByText('Not updated', { exact: true }).waitFor()
      await page.getByText('The provider is unchanged.').waitFor()
      await page.goto(providerPath)
      await waitForProviderStatus(page, 'Records', 'Ready', { available: 2, total: 2 })
      await page.goto(failedUrl)
      await page.getByRole('button', { name: 'Retry', exact: true }).click()
      await page.getByText('Not updated', { exact: true }).waitFor()
      expect(await page.getByRole('button', { name: 'Retry', exact: true }).count()).toBe(0)
      await page.getByRole('link', { name: 'Start over', exact: true }).click()
      await chooseFile(
        page,
        'corrected.json',
        openApiDocument('Records', { listRecords: {}, newReport: {} }),
      )
      await page.getByRole('button', { name: 'Read', exact: true }).click()
      await page.getByRole('button', { name: 'Apply changes', exact: true }).waitFor()
      expect(await page.locator('input[name="requestId"]').last().inputValue()).not.toBe(
        new URL(failedUrl).searchParams.get('requestId'),
      )
      await apply(page)
    })
    await step('A change in another tab invalidates the reviewed request', async () => {
      await upload(openApiDocument('Records', { listRecords: {} }))
      await page.getByRole('button', { name: 'Apply changes', exact: true }).waitFor()
      // Another change moves the provider on, as a second tab or operator would.
      await setProviderEnabled(actor.accessToken, providerPath, false)
      await setProviderEnabled(actor.accessToken, providerPath, true)
      await page.getByRole('button', { name: 'Apply changes', exact: true }).click()
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
