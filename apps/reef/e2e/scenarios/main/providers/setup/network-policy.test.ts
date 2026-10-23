import { createServer } from 'node:http'
import { mkdir } from 'node:fs/promises'
import { withBrowserSession } from '../../../../support/browser-session.ts'
import { startStack } from '../../../../support/stack.ts'
import { join } from 'node:path'
import { expect, inject, test } from 'vitest'
import { withOwnerSession, mcpToken, loginActor } from '../../../../support/actors.ts'
import { listen, openApiDocument } from '../../../../support/provider-fixtures.ts'
import {
  addProviderFromFile,
  createWorkspace,
  createWorkspaceViaUi,
  enableOperation,
  openCustomSource,
  programRunner,
} from '../../../../support/providers.ts'

// The container owns both CAs; only the trusted CA enters its trust stores.
// HTTP permission must not make the untrusted certificate acceptable.
test.for(['http', 'trusted', 'untrusted'] as const)(
  'Authenticated private hostname provider: %s transport',
  async (transport, context) => {
    const { fixtures, tlsDirectory } = inject('reel')
    if (!fixtures || fixtures.advertisedHost !== 'sidecar' || !tlsDirectory)
      throw new Error('Run network acceptance inside the Reel container')
    let calls = 0
    const upstream = await listen(
      context,
      createServer((_request, response) => {
        calls++
        response.writeHead(200, { 'content-type': 'application/json' }).end('{"answer":42}')
      }),
      {
        ...fixtures,
        ...(transport !== 'http'
          ? {
              tls: {
                certFile: join(tlsDirectory, `${transport}.pem`),
                keyFile: join(tlsDirectory, `${transport}.key`),
                caFile: join(tlsDirectory, `${transport}-ca.pem`),
              },
            }
          : {}),
      },
    )
    expect(new URL(upstream.origin).hostname).toBe('sidecar')
    await withOwnerSession(context, async ({ page, target, actor, step }) => {
      const workspace = await createWorkspace(
        actor.accessToken,
        `Network policy ${crypto.randomUUID()}`,
      )
      await step('Publish the private hostname and enable its operation', async () => {
        const path = await addProviderFromFile(
          page,
          workspace,
          openApiDocument('Sidecar', { hello: {} }, { server: upstream.origin }),
          'sidecar',
        )
        await page.goto(path)
        await enableOperation(page, 'hello')
      })
      await step('Dispatch as the signed-in user with normal TLS verification', async () => {
        const token = await mcpToken(page, workspace)
        const claims = JSON.parse(Buffer.from(token.split('.')[1], 'base64url').toString())
        expect(claims.sub).toBe(actor.userId)
        const run = programRunner(page, target, workspace, { accessToken: token })
        let result = await run.exec('return (await coral.providers.sidecar.hello()).data;')
        for (
          let n = 0;
          n < 10 &&
          (result.structuredContent.state === 'running' || result.structuredContent.has_more);
          n++
        )
          result = await run.wait(result.structuredContent)
        expect(result.structuredContent.state).toBe(
          transport === 'untrusted' ? 'failed' : 'succeeded',
        )
        expect(calls).toBe(transport === 'untrusted' ? 0 : 1)
        if (transport !== 'untrusted')
          expect(result.structuredContent.final_value).toEqual({
            kind: 'value',
            value: { answer: 42 },
          })
      })
    })
  },
)

test('Default authenticated policy rejects a private HTTP hostname before discovery', async (context) => {
  const bootstrap = inject('reelBootstrap')
  const config = inject('reel')
  const runDir = join(config.runDir, config.target.name, 'default-policy')
  await mkdir(runDir, { recursive: true })
  // A separate real authenticated stack supplies no outbound override. Do not
  // reuse the permissive product fixture deployment as a default-policy test.
  // This storage-independent control owns a fresh SQLite state, never the
  // product target's live Postgres database or Organization.
  const stack = await startStack({
    ...bootstrap,
    postgresURL: undefined,
    runDir,
    authenticated: true,
  })
  context.onTestFinished(() => stack.stop())
  const authentication = stack.target.authentication!
  const target = { ...stack.target, authentication }
  const actor = await loginActor(context, authentication.owner, target)
  let calls = 0
  const upstream = await listen(
    context,
    createServer((_request, response) => {
      calls++
      response.end('must not be reached')
    }),
  )
  expect(new URL(upstream.origin).hostname).toBe('sidecar')
  await withBrowserSession(
    context,
    async ({ page, step }) => {
      const workspace = await createWorkspaceViaUi(page, `Default policy ${crypto.randomUUID()}`)
      await step('Reject a private hostname under authenticated defaults', async () => {
        await page.goto(`/workspaces/${workspace}/providers/new`)
        await openCustomSource(page)
        await page.getByLabel('OpenAPI URL', { exact: true }).fill(upstream.origin)
        const result = page.waitForResponse(
          (value) => value.request().method() === 'POST' && value.url().includes('/new.data'),
        )
        await page.getByRole('button', { name: 'Read', exact: true }).click()
        expect((await result).status()).toBe(409)
        expect(calls).toBe(0)
      })
    },
    { target, actor: actor.label, storageState: actor.storageState },
  )
})
