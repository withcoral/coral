import type { Page } from 'playwright'
import { expect } from 'vitest'
import type { Target } from '../../support/config.ts'
import { coralInstances } from '../../support/instances.ts'
import { managementClients } from '../../support/management.ts'
import { openApiDocument } from '../../support/provider-fixtures.ts'
import {
  createWorkspace,
  pausedForApproval,
  programRunner,
  type RunUpdate,
} from '../../support/providers.ts'

/**
 * A Program whose one call, addProvider, waits for the invoking User's approval
 * in a new workspace's default policy. Approved, it reaches no upstream: it
 * returns the link where a person signs in to add the provider.
 */
export function addProviderProgram() {
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
  return `try { return await coral.workspace.addProvider(${JSON.stringify(call)}); } catch (error) { return error.code; }`
}

/**
 * Starts addProviderProgram through the Coral Instance `owner`, which then
 * holds the Run, and returns once the Run waits for approval.
 */
export async function runWaitingForApproval(
  page: Page,
  target: Target,
  accessToken: string | undefined,
  owner: number,
) {
  const workspace = await createWorkspace(accessToken, `Instances ${crypto.randomUUID()}`)
  const runner = programRunner(page, target, workspace, { taskIntent: 'Add the weather provider' })
  let paused!: RunUpdate
  const run = await coralInstances().startOn(owner, async () => {
    paused = await pausedForApproval(runner, addProviderProgram())
    return `workspaces/${workspace}/runs/${paused.run_id}`
  })
  const approval = `workspaces/${workspace}/approvals/${paused.required_actions[0].request_id}`
  const inspected = await managementClients(accessToken).activity.inspectApproval({
    name: approval,
  })
  expect([inspected.run, inspected.state]).toEqual([run, 'pending'])
  return { workspace, runner, paused, run, approval }
}
