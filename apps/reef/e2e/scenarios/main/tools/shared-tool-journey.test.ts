import { createServer } from 'node:http'
import type { Locator, Page } from 'playwright'
import { expect, test, type TestContext } from 'vitest'
import { loginActor, mcpToken, newUser, type Actor } from '../../../support/actors.ts'
import { withBrowserSession } from '../../../support/browser-session.ts'
import { managementClients } from '../../../support/management.ts'
import { listen, openApiDocument } from '../../../support/provider-fixtures.ts'
import {
  checkRadio,
  expectArrivalToast,
  observeRun,
  programRunner,
  publishWorkspaceProvider,
} from '../../../support/providers.ts'
import { WorkspaceRole } from '../../../generated/coral/management/v1/workspace_pb.ts'

// One Shared Tool across every process: an Owner publishes it in Reef, a Member's
// MCP agent finds and calls it, Reef's Run page shows the calls its body made, and
// the Owner's restore and removal reach the Member's next Run. The publish,
// restore and removal rules stay in `crates/coral-run` and Reef's Tools page tests.

const PATH = 'accounts.owner'
const SHARED_CALL_PATH = `coral.shared.${PATH}`

/** A tool that asks the directory for the owner, and says which of its versions ran. */
const ownerLookupSource = (marker: string) =>
  [
    'const owner = async () => {',
    '  const { data } = await coral.providers.directory.lookup({})',
    `  return { version: ${JSON.stringify(marker)}, owner: data.owner }`,
    '}',
  ].join('\n')

/** Opens the private tool's drawer and publishes it at its own path, allowed by default. */
async function publishPrivateTool(page: Page, workspace: string, replaces: number | null) {
  await page.goto(`/workspaces/${workspace}/tools/private/${PATH}`)
  const drawer = page.getByRole('dialog', { name: PATH, exact: true })
  await drawer.getByRole('button', { name: 'Publish', exact: true }).click()
  const review = page.getByRole('dialog', { name: 'Publish shared tool', exact: true })
  expect(await review.getByLabel('Shared tool path', { exact: true }).inputValue()).toBe(PATH)
  // The review is ready once it shows the policy, and the revision Coral has at the path.
  await review.getByRole('radio', { name: 'Allow', exact: true }).waitFor()
  if (replaces !== null)
    await review.getByText(`This publish replaces revision ${replaces}`, { exact: false }).waitFor()
  await checkRadio(review.getByRole('radio', { name: 'Allow', exact: true }))
  await expectArrivalToast(
    page,
    { flag: 'sharedToolPublished', title: 'Shared tool published' },
    () => review.getByRole('button', { name: 'Publish', exact: true }).click(),
  )
}

/** An agent's Program that calls the shared tool, and the Run Update it starts. */
const callSharedTool = (agent: ReturnType<typeof programRunner>) =>
  agent.exec(`return await ${SHARED_CALL_PATH}();`)

/** Opens a record's row on the Run page, as a reader does, so its children show under it. */
async function openRecord(row: Locator) {
  await row.getByRole('button', { name: 'Expand record', exact: true }).click()
  await row.getByRole('button', { name: 'Collapse record', exact: true }).waitFor()
}

/** The open shared tool's drawer, once it shows `revision` as current, published by Alice. */
async function sharedToolDrawer(page: Page, revision: number) {
  const drawer = page.getByRole('dialog', { name: PATH, exact: true })
  await drawer.getByText(`Revision ${revision} · published by Alice (You) · `).waitFor()
  return drawer
}

test('An owner publishes a private tool, a member’s agent finds and calls it, and the owner restores and removes it', async (context: TestContext) => {
  const alice = await loginActor(context, await newUser(context, 'Alice'))
  const bob = await loginActor(context, await newUser(context, 'Bob'))
  let lookups = 0
  const upstream = await listen(
    context,
    createServer((_request, response) => {
      lookups++
      response.writeHead(200, { 'content-type': 'application/json' }).end('{"owner":"Dana"}')
    }),
  )
  const api = managementClients(alice.accessToken)
  const workspace = await api.workspaces.createWorkspace({
    displayName: `Shared tools ${crypto.randomUUID()}`,
    requestId: crypto.randomUUID(),
  })
  const id = workspace.name.split('/')[1]!
  await api.memberships.createWorkspaceMembership({
    parent: workspace.name,
    email: bob.email,
    role: WorkspaceRole.MEMBER,
    requestId: crypto.randomUUID(),
  })
  // The tool's body calls a provider tool that every caller may call without an account.
  // A GET tool is allowed by default.
  await publishWorkspaceProvider(
    alice.accessToken,
    workspace.name,
    openApiDocument(
      'Directory',
      { lookup: { summary: 'Look up the account owner' } },
      { server: upstream.origin },
    ),
    'directory',
  )

  /** One session as `actor`, recorded under their label. */
  const as = (actor: Actor, body: Parameters<typeof withBrowserSession>[1]) =>
    withBrowserSession(context, body, { actor: actor.label, storageState: actor.storageState })

  await as(alice, async ({ page, target, step }) => {
    const agent = programRunner(page, target, id, { taskIntent: 'Write the owner lookup' })
    await step('Alice’s agent saves a private tool', async () => {
      const saved = await agent.addCustomTool(
        PATH,
        'Find the owner of an account',
        ownerLookupSource('first'),
        'owner',
      )
      expect(saved.isError, JSON.stringify(saved.structuredContent)).not.toBe(true)
    })
    await step('Alice publishes it as a shared tool in Reef', async () => {
      await publishPrivateTool(page, id, null)
      await sharedToolDrawer(page, 1)
    })
    await step('Alice’s agent changes the private tool, and Alice publishes it again', async () => {
      const saved = await agent.updateCustomTool(
        PATH,
        'Find the owner of an account',
        ownerLookupSource('second'),
        'owner',
      )
      expect(saved.isError, JSON.stringify(saved.structuredContent)).not.toBe(true)
      await publishPrivateTool(page, id, 1)
      await sharedToolDrawer(page, 2)
    })
  })

  // Bob's agent keeps one MCP grant, so each of its later Runs authenticates the same way.
  let bobToken = ''
  /** The Shared Tool Revisions that Bob's Run linked when Coral accepted it. */
  const linkedRevisions = async (runId: string) =>
    (
      await managementClients(bob.accessToken).activity.inspectRun({
        name: `workspaces/${id}/runs/${runId}`,
        pageSize: 1,
      })
    ).linkedSharedTools.map(({ path, revision }) => ({ path, revision }))

  await as(bob, async ({ page, target, step }) => {
    bobToken = await mcpToken(page, id)
    const agent = programRunner(page, target, id, {
      accessToken: bobToken,
      taskIntent: 'Find the account owner',
    })
    let run = ''
    await step('Bob’s agent finds the shared tool with coral.search', async () => {
      await expect
        .poll(
          async () =>
            (await agent.search('owner of an account')).structuredContent.hits.map(
              (hit) => hit.call_path,
            ),
          { timeout: 30_000 },
        )
        .toContain(SHARED_CALL_PATH)
    })
    await step('Bob’s agent calls it and gets the latest revision’s answer', async () => {
      const before = lookups
      const { update } = await observeRun(agent, await callSharedTool(agent))
      expect(update.final_value).toEqual({
        kind: 'value',
        value: { version: 'second', owner: 'Dana' },
      })
      expect(lookups - before).toBe(1)
      run = `workspaces/${id}/runs/${update.run_id}`
      expect(await linkedRevisions(update.run_id)).toEqual([{ path: PATH, revision: 2n }])
    })
    await step('The Run page shows the call the shared tool made inside its own call', async () => {
      await page.goto(`/${run}`)
      const table = page.getByRole('table', { name: 'Activity records', exact: true })
      // The Run page selects the Run; its calls show once it opens.
      await openRecord(table.getByRole('row').filter({ hasText: 'Call one Tool' }))
      await openRecord(
        table
          .getByRole('row')
          .filter({ has: page.getByRole('link', { name: SHARED_CALL_PATH, exact: true }) }),
      )
      // The nested call opens its detail, which names the call and revision that made it.
      const nested = table.getByRole('link', { name: /lookup/ })
      await nested.click()
      const detail = page.getByRole('dialog')
      await detail.getByText('Called from', { exact: true }).waitFor()
      await detail.getByText(`${SHARED_CALL_PATH}, revision 2`, { exact: false }).waitFor()
    })
  })

  await as(alice, async ({ page, step }) => {
    await step('Alice restores revision 1', async () => {
      await page.goto(`/workspaces/${id}/tools/shared/${PATH}`)
      const drawer = await sharedToolDrawer(page, 2)
      await drawer.getByRole('tab', { name: /^Revisions/ }).click()
      await drawer.getByRole('button', { name: 'Restore revision 1', exact: true }).click()
      const review = page.getByRole('dialog', { name: 'Restore revision 1', exact: true })
      await review.getByText('This publish replaces revision 2', { exact: false }).waitFor()
      await expectArrivalToast(
        page,
        { flag: 'sharedToolPublished', title: 'Shared tool published' },
        () => review.getByRole('button', { name: 'Publish', exact: true }).click(),
      )
      const restored = await sharedToolDrawer(page, 3)
      await restored.getByRole('tab', { name: /^Revisions/ }).click()
      await restored.getByText('Restores revision 1', { exact: true }).waitFor()
    })
  })

  await as(bob, async ({ page, target, step }) => {
    const agent = programRunner(page, target, id, {
      accessToken: bobToken,
      taskIntent: 'Find the account owner again',
    })
    await step('Bob’s next call runs the restored revision', async () => {
      const { update } = await observeRun(agent, await callSharedTool(agent))
      expect(update.final_value).toEqual({
        kind: 'value',
        value: { version: 'first', owner: 'Dana' },
      })
      // The restore is a new revision with revision 1's content.
      expect(await linkedRevisions(update.run_id)).toEqual([{ path: PATH, revision: 3n }])
    })
  })

  await as(alice, async ({ page, step }) => {
    await step('Alice removes the shared tool', async () => {
      await page.goto(`/workspaces/${id}/tools/shared/${PATH}`)
      const drawer = await sharedToolDrawer(page, 3)
      await drawer.getByRole('button', { name: 'More actions', exact: true }).click()
      await page.getByRole('menuitem', { name: 'Remove shared tool', exact: true }).click()
      const confirm = page.getByRole('dialog', { name: 'Remove shared tool', exact: true })
      await expectArrivalToast(
        page,
        { flag: 'sharedToolRemoved', title: 'Shared tool removed' },
        () => confirm.getByRole('button', { name: 'Remove shared tool', exact: true }).click(),
      )
    })
  })

  await as(bob, async ({ page, target, step }) => {
    const agent = programRunner(page, target, id, {
      accessToken: bobToken,
      taskIntent: 'Find the account owner after the removal',
    })
    await step('Bob’s next Program fails at admission', async () => {
      const before = lookups
      // Coral refuses the Program before it runs: it names a tool nobody can call.
      expect(await callSharedTool(agent)).toMatchObject({
        isError: true,
        structuredContent: { code: 'program_admission_failed' },
      })
      expect(lookups).toBe(before)
    })
  })
})
