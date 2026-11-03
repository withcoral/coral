import { expect, test } from 'vitest'
import { Code } from '@connectrpc/connect'
import { createActors } from '../../../support/actors.ts'
import { withBrowserSession } from '../../../support/browser-session.ts'
import { managementClients } from '../../../support/management.ts'
import { prepareOffboardingAccount } from '../../../support/offboarding.ts'
import { breadcrumb, dismissOnboarding, programRunner } from '../../../support/providers.ts'
import { SetupAuthorizationState } from '../../../generated/coral/management/v1/provider_pb.ts'
import { copyNewAgentKey } from '../../../support/agent-key.ts'
import { copiedBy } from '../../../support/clipboard.ts'

test('Agent credential issuance, rotation, disablement, expiry and destructive cleanup', async (context) => {
  const { owner } = await createActors(context)
  const api = managementClients(owner.accessToken)
  await withBrowserSession(
    context,
    async ({ page, target, step }) => {
      let oldCredential = ''
      let newCredential = ''
      let workspace = ''
      let agentPath = ''
      let currentWork: Awaited<ReturnType<typeof prepareOffboardingAccount>>
      let retainedTask = ''
      const authenticate = async (
        credential: string,
        endpoint = `${target.mcpURL}/workspaces/${workspace}/mcp`,
        authorization = `Bearer ${credential}`,
      ) => {
        const response = await page.request.post(endpoint, {
          headers: {
            Authorization: authorization,
            Accept: 'application/json, text/event-stream',
            'MCP-Protocol-Version': '2025-11-25',
          },
          data: {
            jsonrpc: '2.0',
            id: 1,
            method: 'initialize',
            params: {
              protocolVersion: '2025-11-25',
              capabilities: {},
              clientInfo: { name: 'recorded-lifecycle', version: '1' },
            },
          },
        })
        return response.status()
      }
      await step('Create an Agent and save its one-time credential', async () => {
        const name = `Agent lifecycle ${crypto.randomUUID()}`
        await page.goto('/')
        await page.getByRole('button', { name: /^Open account menu for / }).waitFor()
        const nav = page.getByRole('navigation', { name: 'Coral', exact: true })
        await nav.getByRole('button', { name: 'Create workspace', exact: true }).click()
        await page.getByLabel('Workspace name', { exact: true }).fill(name)
        await page
          .getByRole('dialog')
          .getByRole('button', { name: 'Create workspace', exact: true })
          .click()
        await dismissOnboarding(page)
        // Home may have opened another Workspace in the tree; use the new one.
        await nav
          .getByRole('group', { name, exact: true })
          .getByRole('link', { name: 'Agents', exact: true })
          .click()
        // The Overview has its own Create agent button; act only once Agents has loaded.
        await page.getByRole('heading', { name: 'Agents', exact: true, level: 1 }).waitFor()
        await page.getByRole('button', { name: 'Create agent', exact: true }).click()
        await page.getByLabel('Agent name', { exact: true }).fill('Automation')
        workspace = new URL(page.url()).pathname.split('/')[2]
        await page
          .getByRole('dialog')
          .getByRole('button', { name: 'Create agent', exact: true })
          .click()
        oldCredential = await copyNewAgentKey(page)
        expect(await authenticate(oldCredential)).toBe(200)
        await page.getByRole('link', { name: 'Automation', exact: true }).click()
        await breadcrumb(page, 'Automation').waitFor()
        agentPath = new URL(page.url()).pathname
        currentWork = await prepareOffboardingAccount(
          owner.accessToken,
          owner.accessToken,
          `workspaces/${workspace}`,
          decodeURIComponent(agentPath.slice(1)),
        )
        const runner = programRunner(page, target, workspace, {
          accessToken: oldCredential,
          taskIntent: 'Retained Agent lifecycle work',
        })
        expect(
          (await runner.saveFunction('answer', 'Agent answer', 'export default () => 42;')).isError,
        ).not.toBe(true)
        expect(
          (await runner.exec('return coral.functions.answer();')).structuredContent.final_value,
        ).toEqual({ kind: 'value', value: 42 })
        retainedTask = `workspaces/${workspace}/tasks/${runner.taskId()}`
      })
      await step('Open settings from the activity page and from the Agents list', async () => {
        await page.getByRole('link', { name: 'Settings', exact: true }).click()
        await page.waitForURL((url) => url.pathname === `${agentPath}/settings`)
        await breadcrumb(page, 'Settings').waitFor()
        await breadcrumb(page).getByRole('link', { name: 'Agents', exact: true }).click()
        await page.getByRole('link', { name: 'Automation settings', exact: true }).click()
        await page.waitForURL((url) => url.pathname === `${agentPath}/settings`)
        await breadcrumb(page).getByRole('link', { name: 'Automation', exact: true }).waitFor()
      })
      await step('Rotate once and prove the old credential no longer authenticates', async () => {
        await page.getByRole('button', { name: 'Replace API key', exact: true }).click()
        await page
          .getByRole('dialog')
          .getByRole('button', { name: 'Replace API key', exact: true })
          .click()
        // The replaced key's dialog fills the served endpoint and the new key into a client's setup.
        const dialog = page.getByRole('dialog')
        await dialog.getByRole('tab', { name: 'Other', exact: true }).click()
        const endpoint = await copiedBy(
          page,
          dialog.getByRole('button', { name: 'Copy MCP URL', exact: true }),
        )
        const authorization = await copiedBy(
          page,
          dialog.getByRole('button', { name: 'Copy Authorization header', exact: true }),
        )
        newCredential = await copyNewAgentKey(page)
        expect(newCredential !== oldCredential).toBe(true)
        expect(await authenticate(newCredential, endpoint, authorization)).toBe(200)
        expect(await authenticate(oldCredential)).toBe(401)
        expect(await authenticate(newCredential)).toBe(200)
        expect(decodeURIComponent(new URL(page.url()).pathname)).toBe(
          decodeURIComponent(`${agentPath}/settings`),
        )
        const response = await page.reload()
        expect(response?.headers()['cache-control']).toContain('no-store')
        expect((await page.content()).includes(newCredential)).toBe(false)
      })
      await step('Disable Agent authority and explicitly enable it again', async () => {
        await page.getByRole('button', { name: 'Disable agent', exact: true }).click()
        await page.getByText('Agent disabled', { exact: true }).first().waitFor()
        expect(await authenticate(newCredential)).toBe(401)
        await page.getByRole('button', { name: 'Enable agent', exact: true }).click()
        await page.getByRole('button', { name: 'Disable agent', exact: true }).waitFor()
        expect(await authenticate(newCredential)).toBe(200)
      })
      await step('Expire the Registration and clear its expiration explicitly', async () => {
        await page.getByRole('button', { name: 'Edit expiration', exact: true }).click()
        await page.getByLabel('Access expires (UTC)', { exact: true }).fill('2020-01-01T00:00')
        await page.getByRole('button', { name: 'Save expiration', exact: true }).click()
        await page.getByText('Access expired', { exact: true }).waitFor()
        expect(await authenticate(newCredential)).toBe(401)
        await page.getByRole('button', { name: 'Edit expiration', exact: true }).click()
        await page.getByLabel('Access expires (UTC)', { exact: true }).fill('')
        await page.getByRole('button', { name: 'Save expiration', exact: true }).click()
        await page.getByText('Never', { exact: true }).waitFor()
        expect(await authenticate(newCredential)).toBe(200)
      })
      await step('Registration disablement is separate from Agent state', async () => {
        await page.getByRole('button', { name: 'Suspend API access', exact: true }).click()
        await page.getByText('API access suspended', { exact: true }).waitFor()
        await page.getByRole('button', { name: 'Disable agent', exact: true }).waitFor()
        expect(await authenticate(newCredential)).toBe(401)
      })
      await step('Confirm irreversible current Agent cleanup', async () => {
        await page.getByRole('button', { name: 'Delete agent', exact: true }).click()
        await page
          .getByRole('dialog')
          .getByRole('button', { name: 'Delete agent', exact: true })
          .click()
        await page.waitForURL((url) => url.pathname === `/workspaces/${workspace}/agents`)
        const name = decodeURIComponent(agentPath.slice(1))
        await expect(api.agents.getAgent({ name })).rejects.toMatchObject({ code: Code.NotFound })
        await expect(
          api.registrations.getAgentRegistration({ name: `${name}/registration` }),
        ).rejects.toMatchObject({ code: Code.NotFound })
        expect(await authenticate(newCredential)).toBe(401)
        expect(await page.getByRole('link', { name: 'Automation', exact: true }).count()).toBe(0)
        expect(
          (await api.accounts.getProviderAccountAuthorization({ name: currentWork.pending })).state,
        ).toBe(SetupAuthorizationState.CANCELED)
        expect((await api.tasks.getTask({ name: retainedTask })).name).toBe(retainedTask)
        await expect(
          api.accounts.startProviderAccountAuthorization({
            parent: `workspaces/${workspace}`,
            requestId: crypto.randomUUID(),
            redirectUri: `${target.baseURL}/provider-setup/oauth/callback`,
            target: {
              case: 'reconnect',
              value: { provider: currentWork.provider, account: currentWork.account },
            },
          }),
        ).rejects.toMatchObject({ code: Code.NotFound })
      })
      await step(
        'Inspect retained Agent activity without restoring management controls',
        async () => {
          const response = await page.goto(agentPath)
          expect(response?.status()).toBe(200)
          await breadcrumb(page, 'Automation').waitFor()
          await page
            .getByText('This agent cannot start new work', {
              exact: true,
            })
            .waitFor()
          for (const name of [
            'Delete agent',
            'Replace API key',
            'Enable agent',
            'Disable agent',
            'Edit expiration',
            'Suspend API access',
          ])
            expect(await page.getByRole('button', { name, exact: true }).count()).toBe(0)
          expect(await page.getByRole('link', { name: 'Settings', exact: true }).count()).toBe(0)
          await page
            .getByRole('link', { name: 'Retained Agent lifecycle work', exact: true })
            .click()
          await page.waitForURL((url) => url.searchParams.get('record') === retainedTask)
          expect((await page.goto(`${agentPath}/settings`))?.status()).toBe(404)
          await page.getByText('This page does not exist.', { exact: true }).waitFor()
        },
      )
    },
    { actor: owner.label, storageState: owner.storageState },
  )
})
