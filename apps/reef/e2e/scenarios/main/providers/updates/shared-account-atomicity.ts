import { toJsonString } from '@bufbuild/protobuf'
import { expect, type TestContext } from 'vitest'
import type { Page } from 'playwright'
import { inspectRunEvents, managementClients } from '../../../../support/management.ts'
import {
  ProviderAccountSchema,
  ProviderAccountScope,
  ProviderAccountState,
} from '../../../../generated/coral/management/v1/account_pb.ts'
import { collectDataResponses } from '../../../../support/data-responses.ts'
import { oauthMcpServer } from '../../../../support/provider-fixtures.ts'
import {
  addedProviders,
  acceptTools,
  checkRadio,
  chooseAccount,
  confirmConfiguration,
  createWorkspace,
  observeRetainedRun,
  observeRun,
  openCustomSource,
  openProviderSettings,
  programRunner,
  discardSettingsChange,
  saveConfirmedSettingsChange,
  signInToReadTools,
  waitForPublication,
} from '../../../../support/providers.ts'
import { withOwner } from '../../../../support/topology.ts'

// Every change returns to the provider's settings, where it opens.
const save = async (page: Page) => {
  expect(new URL(page.url()).searchParams.has('toolsAccepted')).toBe(false)
  await page.getByRole('button', { name: 'Save', exact: true }).click()
  await page.waitForURL((url) => url.pathname.endsWith('/settings'))
}

/** Failed incompatible shared updates preserve the active configuration and account. */
export async function sharedAccountAtomicity(context: TestContext) {
  const oldSecret = `old-update-${crypto.randomUUID()}`
  const candidateSecret = `candidate-update-${crypto.randomUUID()}`
  const active = await oauthMcpServer(context, oldSecret)
  const candidate = await oauthMcpServer(context, candidateSecret, {
    additionalScopes: ['additional.permission'],
  })
  await withOwner(context, async ({ page, target, step, accessToken, approveConsent }) => {
    const api = managementClients(accessToken)
    const responses = await collectDataResponses(page)
    const safeResponses: string[] = []
    const workspace = await createWorkspace(accessToken, `Atomic Update ${crypto.randomUUID()}`)
    const parent = `workspaces/${workspace}`
    await step('Publish a working shared provider through OAuth', async () => {
      await page.goto(`/workspaces/${workspace}/providers/new`)
      await openCustomSource(page)
      await page.getByLabel('MCP server URL', { exact: true }).fill(active.mcpUrl)
      await page.getByRole('button', { name: 'Connect', exact: true }).click()
      await page.getByLabel('Namespace', { exact: true }).fill('atomicupdate')
      await confirmConfiguration(page)
      await signInToReadTools(page)
      await approveConsent()
      await acceptTools(page)
      await chooseAccount(page, 'shared')
      await page.getByRole('button', { name: 'Add provider', exact: true }).click()
      await waitForPublication(page)
    })
    const providerPath = new URL(page.url()).pathname
    await step('Refresh the saved source using the existing shared account', async () => {
      await openProviderSettings(page)
      await page.getByRole('link', { name: 'Check for updates', exact: true }).click()
      await page.getByText(active.mcpUrl, { exact: true }).waitFor()
      expect(await page.getByRole('button', { name: 'Change source', exact: true }).count()).toBe(0)
      expect(
        await page.getByRole('textbox', { name: /^(OpenAPI URL|MCP server URL)$/ }).count(),
      ).toBe(0)
      await page.getByRole('button', { name: 'Read tools', exact: true }).click()
      await page.getByRole('heading', { name: 'Access to read tools', exact: true }).waitFor()
      expect(await page.getByRole('textbox', { name: 'Display name', exact: true }).count()).toBe(0)
      await page
        .getByRole('button', { name: 'Use the existing shared account', exact: true })
        .click()
      // Keeping the shared account needs no consent, so Save publishes at once.
      await save(page)
      expect(active.counts.consents).toBe(1)
      expect(active.counts.tokens).toBe(1)
    })
    await step('A changed issuer needs review before any new sign-in', async () => {
      active.knobs.issuerPath = '/new-issuer'
      await openProviderSettings(page)
      await page.getByRole('link', { name: 'Check for updates', exact: true }).click()
      await page.getByRole('button', { name: 'Read tools', exact: true }).click()
      // The configuration step opens; the page before the read has no fields.
      await page.getByLabel('Display name', { exact: true }).waitFor()
      expect(await page.getByRole('button', { name: /^Sign in/ }).count()).toBe(0)
      expect(active.counts.consents).toBe(1)
      const setupName = `${parent}/providerSetups/${new URL(page.url()).searchParams.get('setup')}`
      expect(
        (await api.setups.getProviderSetup({ name: setupName })).configuration?.interruptsAccess,
      ).toBe(true)
      // The read is unsaved, so Cancel asks first.
      await discardSettingsChange(page)
      expect(new URL(page.url()).pathname).toBe(`${providerPath}/settings`)
      active.knobs.issuerPath = undefined
    })
    const [provider] = addedProviders((await api.providers.listProviders({ parent })).providers)
    const inspectAccount = () =>
      api.accounts.getProviderAccount({
        parent,
        provider: provider.name,
        scope: ProviderAccountScope.SHARED,
      })
    const account = await inspectAccount()
    expect(account.account!.id).toBeTruthy()
    expect(account.account!.state).toBe(ProviderAccountState.READY)
    safeResponses.push(toJsonString(ProviderAccountSchema, account.account!))
    const run = programRunner(page, target, workspace)
    const runIds = new Set<string>()
    const invocationIds = new Set<string>()
    const executeOld = async () => {
      const description = await run.describe('coral.providers.atomicupdate.lookup')
      expect(description.isError).not.toBe(true)
      safeResponses.push(JSON.stringify(description))
      const result = await run.exec(
        'coral.emit("once"); return await coral.providers.atomicupdate.lookup({});',
      )
      const observation = await observeRun(run, result)
      const { update, events } = observation
      safeResponses.push(...observation.responses)
      const id = update.run_id
      expect(runIds.has(id)).toBe(false)
      runIds.add(id)
      expect(JSON.stringify(update.final_value)).toContain('Hello from the OAuth fixture')
      const history = await inspectRunEvents(api, `workspaces/${workspace}/runs/${id}`)
      const starts = history.flatMap((e) =>
        e.payload.case === 'invocationStarted' ? [e.payload.value] : [],
      )
      const ends = history.flatMap((e) =>
        e.payload.case === 'invocationEnded' ? [e.payload.value] : [],
      )
      expect(starts).toHaveLength(1)
      expect(ends).toHaveLength(1)
      expect(starts[0].invocation).toBeTruthy()
      expect(ends[0].invocation).toBe(starts[0].invocation)
      expect(invocationIds.has(starts[0].invocation)).toBe(false)
      invocationIds.add(starts[0].invocation)
      expect(events.filter((event) => event.kind === 'program.emitted')).toHaveLength(1)
      expect(history.filter((e) => e.payload.case === 'started')).toHaveLength(1)
      // Replay authored output from another MCP session without executing again.
      const observer = programRunner(page, target, workspace)
      const retained = await observeRetainedRun(observer, observation)
      safeResponses.push(...retained.responses)
      expect(run.calls.exec).toBe(runIds.size)
      expect(observer.calls.exec ?? 0).toBe(0)
      expect(active.counts.calls).toBe(runIds.size)
      expect(candidate.counts.calls).toBe(0)
    }
    await executeOld()
    for (const failure of ['authorization', 'publication'] as const) {
      await step(`Preserve the active pair after ${failure} failure`, async () => {
        candidate.knobs.tokenError = failure === 'authorization' ? 'access_denied' : undefined
        candidate.knobs.invalidCatalog = failure === 'publication'
        await page.goto(`${providerPath}/settings`)
        await page.getByRole('link', { name: 'Replace source', exact: true }).click()
        await openCustomSource(page)
        await page.getByLabel('MCP server URL', { exact: true }).fill(candidate.mcpUrl)
        await page.getByRole('button', { name: 'Connect', exact: true }).click()
        await confirmConfiguration(page)
        await page.getByRole('button', { name: /^Sign in/ }).click()
        await approveConsent()
        const setupName = `${parent}/providerSetups/${new URL(page.url()).searchParams.get('setup')!}`
        if (failure === 'authorization') {
          await page.getByRole('button', { name: 'Sign in again', exact: true }).waitFor()
          expect((await api.setups.getProviderSetup({ name: setupName })).authorized).toBe(false)
          // The confirmed configuration is unsaved, so Cancel asks first.
          await discardSettingsChange(page)
          expect(new URL(page.url()).pathname).toBe(`${providerPath}/settings`)
          expect(new URL(page.url()).pathname).toBe(`${providerPath}/settings`)
        } else {
          await page.getByText(/Granted permissions:.*additional\.permission/).waitFor()
          await checkRadio(page.getByRole('radio', { name: 'Use this account', exact: true }))
          expect((await api.setups.getProviderSetup({ name: setupName })).authorized).toBe(true)
          const failedUrl = new URL(
            (await page.locator('form:has(input[value="publish"])').getAttribute('action'))!,
            page.url(),
          ).toString()
          // A new shared account replaces the old one, so Save asks first.
          await page.getByRole('button', { name: 'Save', exact: true }).click()
          await page
            .getByRole('dialog', { name: 'Save changes?' })
            .getByRole('button', { name: 'Save', exact: true })
            .click()
          await page.getByText('Not updated', { exact: true }).waitFor()
          expect((await api.setups.getProviderSetup({ name: setupName })).authorized).toBe(false)
          // A replay of the failed request returns the same failure.
          await page.goto(failedUrl)
          await page.getByRole('button', { name: 'Retry', exact: true }).click()
          await page.getByText('Not updated', { exact: true }).waitFor()
          expect(await page.getByRole('button', { name: 'Retry', exact: true }).count()).toBe(0)
        }
        expect(await api.providers.getProvider({ name: provider.name })).toEqual(provider)
        expect(await inspectAccount()).toEqual(account)
        expect(
          (
            await api.accounts.getProviderAccount({
              parent,
              provider: provider.name,
              scope: ProviderAccountScope.USER,
            })
          ).account,
        ).toBeUndefined()
        await executeOld()
      })
    }
    expect(active.counts.calls).toBe(3)
    expect(candidate.counts.tokens).toBe(2)
    expect(candidate.counts.calls).toBe(0)
    await step('Change account settings without restoring the old authorization', async () => {
      await page.goto(`${providerPath}/settings`)
      await page.getByRole('button', { name: 'Change account setting', exact: true }).click()
      await page.getByRole('radio', { name: 'Use individual accounts', exact: true }).check()
      await saveConfirmedSettingsChange(page)
      expect((await inspectAccount()).account).toBeUndefined()
      expect(
        (
          await api.accounts.getProviderAccount({
            parent,
            provider: provider.name,
            scope: ProviderAccountScope.USER,
          })
        ).account,
      ).toBeUndefined()
      await page.getByRole('button', { name: 'Change account setting', exact: true }).click()
      await page.getByRole('radio', { name: 'Connect an account', exact: true }).check()
      // A new shared account is connected from a confirmation, as in Add provider.
      expect(await page.getByRole('button', { name: 'Save', exact: true }).count()).toBe(0)
      await page.getByRole('button', { name: 'Connect the shared account', exact: true }).click()
      await page
        .getByRole('dialog')
        .getByRole('button', { name: 'Sign in and share the account', exact: true })
        .click()
      await approveConsent()
      await checkRadio(page.getByRole('radio', { name: 'Use this account', exact: true }))
      await saveConfirmedSettingsChange(page)
      const changed = await api.providers.getProvider({ name: provider.name })
      expect(changed.name).toBe(provider.name)
      expect(changed.namespace).toBe(provider.namespace)
      expect(active.counts.consents).toBe(2)
      expect(active.counts.tokens).toBe(2)
      await executeOld()
    })
    expect(runIds.size).toBe(4)
    expect(invocationIds.size).toBe(4)
    for (const response of [...(await responses()), ...safeResponses]) {
      expect(response).not.toContain(oldSecret)
      expect(response).not.toContain(candidateSecret)
      expect(response).not.toContain('disposable-code')
    }
  })
}
