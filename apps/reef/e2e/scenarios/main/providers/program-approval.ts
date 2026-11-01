import type { Page } from 'playwright'
import { expect, type TestContext } from 'vitest'
import { shownArguments } from '../../../support/arguments.ts'
import { managementClients } from '../../../support/management.ts'
import { openApiDocument } from '../../../support/provider-fixtures.ts'
import {
  acceptTools,
  addedProviders,
  connectSharedAccount,
  observeRun,
  openReview,
  pausedForApproval,
  programRunner,
  waitForPublication,
} from '../../../support/providers.ts'
import { withOwner } from '../../../support/topology.ts'

/**
 * Starts addProvider with a shared bearer account in a new workspace, which
 * keeps the default policy: the User who runs a Program approves each call.
 * Returns the call, the runner and the paused Run Update.
 */
async function pauseForApproval(
  page: Page,
  target: Parameters<typeof programRunner>[1],
  accessToken: string | undefined,
  displayName: string,
) {
  const workspace = await managementClients(accessToken).workspaces.createWorkspace({
    displayName,
    requestId: crypto.randomUUID(),
  })
  const id = workspace.name.split('/')[1]
  const document = openApiDocument(
    'Weather',
    { getForecast: { summary: 'Get the forecast' } },
    { security: 'bearer' },
  ).toString()
  const call = {
    namespace: 'weather',
    source: { kind: 'openapi_document', document },
    credentialRoute: 'fixed',
  }
  const runner = programRunner(page, target, id, { taskIntent: 'Add the weather provider' })
  const paused = await pausedForApproval(
    runner,
    `try { return await coral.workspace.addProvider(${JSON.stringify(call)}); } catch (error) { return error.code; }`,
  )
  return { workspace, id, call, runner, paused }
}

/**
 * The default path for a Program's new provider: the owner approves the call
 * in Reef, then follows the returned link to sign in and add it. The link names
 * the Reef origin of the deployment the Program ran in.
 */
export async function approvedProgramProvider(context: TestContext) {
  await withOwner(context, async ({ page, target, accessToken, step }) => {
    const api = managementClients(accessToken)
    let started!: Awaited<ReturnType<typeof pauseForApproval>>
    await step('The Run waits for the owner to approve addProvider', async () => {
      started = await pauseForApproval(page, target, accessToken, 'Approved provider')
    })
    const { workspace, id, call, runner, paused } = started
    await step('Review the namespace, route and source, document and all, as sent', async () => {
      const review = await openReview(page, paused, 'addProvider')
      // One row per field, each its exact JSON: the whole call as the Program sent it, with the
      // document inside it and no second copy apart.
      expect(await shownArguments(review)).toEqual(call)
      expect(await review.locator('summary', { hasText: 'OpenAPI document' }).count()).toBe(0)
    })
    let link = ''
    await step('Approve the call, and the Program gets a link where you sign in', async () => {
      await page.getByRole('button', { name: 'Approve this call', exact: true }).click()
      await page.getByText('Approved', { exact: true }).waitFor()
      const { update } = await observeRun(runner, await runner.wait(paused))
      const { data } = (update.final_value as { value: { data: { status: string; url: string } } })
        .value
      expect(data.status).toBe('needs_person')
      link = data.url
      expect(new URL(link).origin).toBe(new URL(target.baseURL).origin)
      expect(new URL(link).pathname).toBe(`/workspaces/${id}/providers/new`)
    })
    await step('Review the tools, connect the account, and add the provider', async () => {
      await page.goto(link)
      await acceptTools(page)
      await connectSharedAccount(page, 'weather-token')
      const add = page.getByRole('button', { name: 'Add provider', exact: true })
      await add.waitFor()
      await add.click()
      await waitForPublication(page)
    })
    await step('Show the provider in the workspace', async () => {
      const providers = addedProviders(
        (await api.providers.listProviders({ parent: workspace.name })).providers,
      )
      expect(providers.map((provider) => provider.callRoot)).toEqual(['coral.providers.weather'])
    })
  })
}

/** A denied call is a failure the Program catches, and it adds nothing. */
export async function deniedProgramProvider(context: TestContext) {
  await withOwner(context, async ({ page, target, accessToken, step }) => {
    const api = managementClients(accessToken)
    let started!: Awaited<ReturnType<typeof pauseForApproval>>
    await step('The Run waits for the owner to approve addProvider', async () => {
      started = await pauseForApproval(page, target, accessToken, 'Denied provider')
    })
    const { workspace, runner, paused } = started
    await step('Deny the call, and the Program catches the failure', async () => {
      const review = await openReview(page, paused, 'addProvider')
      await review.getByRole('button', { name: 'Deny this call', exact: true }).click()
      await page.getByText('Denied', { exact: true }).waitFor()
      const { update } = await observeRun(runner, await runner.wait(paused))
      expect(update.final_value).toEqual({ kind: 'value', value: 'approval_declined' })
    })
    await step('Add no provider and leave nothing to review', async () => {
      const providers = addedProviders(
        (await api.providers.listProviders({ parent: workspace.name })).providers,
      )
      expect(providers).toEqual([])
      const pending = await api.activity.browseRequiredActions({ workspace: workspace.name })
      expect(pending.actionCount).toBe(0n)
    })
  })
}
