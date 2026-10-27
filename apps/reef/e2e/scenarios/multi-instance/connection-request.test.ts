import { expect, test } from 'vitest'
import { coralInstances, expectNoUnavailable } from '../../support/instances.ts'
import { managementClients } from '../../support/management.ts'
import { openApiDocument } from '../../support/provider-fixtures.ts'
import {
  addProviderFromFile,
  createWorkspace,
  programRunner,
  type RunUpdate,
} from '../../support/providers.ts'
import { withOwner } from '../../support/topology.ts'

test('An account connected through one Coral Instance continues a Run that the other owns', async (context) => {
  const instances = coralInstances()
  const owner = 0
  const requests = await instances.during(() =>
    withOwner(context, async ({ page, step, target, accessToken }) => {
      const api = managementClients(accessToken)
      let workspace = ''
      const pendingConnections = async () =>
        (await api.accounts.listUserAccounts({ parent: `workspaces/${workspace}` })).accounts
          .map((row) => row.pendingConnection)
          .filter(Boolean).length
      let run = ''
      let update!: RunUpdate
      let program!: ReturnType<typeof programRunner>
      await step('A Run on the first Coral Instance waits for you to connect', async () => {
        workspace = await createWorkspace(accessToken, `Connections ${crypto.randomUUID()}`)
        await addProviderFromFile(
          page,
          workspace,
          openApiDocument(
            'Connections',
            { listEntries: { summary: 'List entries', method: 'post' } },
            {},
          ),
          'connections',
          { authentication: 'Bearer token', account: 'own-later' },
        )
        program = programRunner(page, target, workspace, { waitMs: 100 })
        run = await instances.startOn(owner, async () => {
          update = (await program.exec('return await coral.providers.connections.listEntries({});'))
            .structuredContent
          return `workspaces/${workspace}/runs/${update.run_id}`
        })
        await expect.poll(pendingConnections, { timeout: 15_000 }).toBe(1)
        expect(await instances.holder(run)).toBe(owner)
      })
      await step('Connect from Accounts through the other Coral Instance', async () => {
        const sent = await instances.during(() =>
          instances.on(instances.other(owner), async () => {
            await page.goto(`/workspaces/${workspace}/accounts`)
            await page.getByRole('button', { name: 'Connect Connections', exact: true }).click()
            const dialog = page.getByRole('dialog', { name: 'Connect Connections', exact: true })
            await dialog.getByLabel('Token', { exact: true }).fill('connection-secret-canary')
            await dialog.getByRole('button', { name: 'Connect account', exact: true }).click()
            await page.waitForURL((url) => url.pathname === `/workspaces/${workspace}/accounts`)
            await page.getByText('Connected', { exact: true }).waitFor()
          }),
        )
        const grpc = sent.filter(({ front }) => front === 'grpc')
        expect(grpc.length).toBeGreaterThan(0)
        expect(new Set(grpc.map(({ instance }) => instance))).toEqual(
          new Set([instances.other(owner)]),
        )
      })
      await step('The waiting call continues to its approval on the owner', async () => {
        await expect
          .poll(
            async () => {
              update = (await program.wait(update)).structuredContent
              return update.required_actions.map(({ kind }) => kind)
            },
            { timeout: 15_000 },
          )
          .toEqual(['invocation_approval'])
        expect(await pendingConnections()).toBe(0)
        expect(await instances.holder(run)).toBe(owner)
      })
    }),
  )
  expectNoUnavailable(requests)
})
