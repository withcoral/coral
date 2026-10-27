import type { Page } from 'playwright'
import { expect, test } from 'vitest'
import { mcpToken } from '../../support/actors.ts'
import { coralInstances, expectNoUnavailable } from '../../support/instances.ts'
import { managementClients } from '../../support/management.ts'
import { createWorkspace, type RunUpdate } from '../../support/providers.ts'
import { withOwner } from '../../support/topology.ts'
import { addProviderProgram } from './waiting-run.ts'

const VERSION = '2026-07-28'

/**
 * A 2026-07-28 MCP client that answers form prompts. It holds no session, so
 * every call may reach either Coral Instance; the proxy alternates them.
 */
function promptingClient(page: Page, endpoint: string, accessToken: string) {
  let id = 0
  return async (name: string, args: object, retry: object = {}) => {
    const response = await page.request.post(endpoint, {
      headers: {
        Accept: 'application/json, text/event-stream',
        Authorization: `Bearer ${accessToken}`,
        'MCP-Protocol-Version': VERSION,
        'Mcp-Method': 'tools/call',
        'Mcp-Name': name,
      },
      data: {
        jsonrpc: '2.0',
        id: ++id,
        method: 'tools/call',
        params: {
          name,
          arguments: args,
          ...retry,
          _meta: {
            'io.modelcontextprotocol/protocolVersion': VERSION,
            'io.modelcontextprotocol/clientInfo': { name: 'Reel', version: '1' },
            'io.modelcontextprotocol/clientCapabilities': { elicitation: { form: {} } },
          },
        },
      },
      timeout: 30_000,
    })
    expect(response.status(), `MCP ${name} HTTP status`).toBe(200)
    const text = await response.text()
    const body = JSON.parse(
      text.startsWith('{')
        ? text
        : text
            .split('\n')
            .find((line) => line.startsWith('data:') && line.slice(5).trim().startsWith('{'))!
            .slice(5),
    )
    expect(body.error).toBeUndefined()
    return body.result
  }
}

test('An MCP client on 2026-07-28 approves through the Coral Instance that does not own the Run', async (context) => {
  const instances = coralInstances()
  const owner = 1
  const requests = await instances.during(() =>
    withOwner(context, async ({ page, step, target, accessToken }) => {
      const api = managementClients(accessToken)
      const workspace = await createWorkspace(accessToken, `Prompts ${crypto.randomUUID()}`)
      const call = promptingClient(
        page,
        `${target.mcpURL}/workspaces/${workspace}/mcp`,
        await mcpToken(page, workspace),
      )
      let args!: object
      let asked!: { inputRequests: Record<string, unknown>; requestState: string }
      let run = ''
      await step('exec on the second Coral Instance asks the client to approve', async () => {
        run = await instances.startOn(owner, async () => {
          const task = await call('start_task', { intent: 'Add the weather provider' })
          args = {
            task_id: task.structuredContent.task_id,
            intent: 'Add a provider',
            program: addProviderProgram(),
            wait_ms: 8000,
          }
          asked = await call('exec', args)
          expect(asked).toMatchObject({ resultType: 'input_required' })
          const [key] = Object.keys(asked.inputRequests)
          expect(key).toMatch(/^approval\//)
          const approval = `workspaces/${workspace}/approvals/${key.slice('approval/'.length)}`
          return (await api.activity.inspectApproval({ name: approval })).run
        })
        expect(await instances.holder(run)).toBe(owner)
      })
      let update!: RunUpdate
      await step('Answer the prompt through the other Coral Instance', async () => {
        const [key] = Object.keys(asked.inputRequests)
        const sent = await instances.during(() =>
          instances.on(instances.other(owner), async () => {
            const answered = await call('exec', args, {
              requestState: asked.requestState,
              inputResponses: { [key]: { action: 'accept' } },
            })
            expect(answered.isError).not.toBe(true)
            update = answered.structuredContent
          }),
        )
        expect(sent.filter(({ front }) => front === 'mcp')).toEqual([
          expect.objectContaining({ instance: instances.other(owner), status: 200 }),
        ])
        const approval = `workspaces/${workspace}/approvals/${key.slice('approval/'.length)}`
        expect((await api.activity.inspectApproval({ name: approval })).state).toBe('approved')
      })
      await step('The owner records the approval, and the Program finishes', async () => {
        for (let pages = 0; pages < 10 && (update.state === 'running' || update.has_more); pages++)
          update = (
            await call('wait', {
              run_id: update.run_id,
              cursor: update.next_cursor || undefined,
              wait_ms: 8000,
            })
          ).structuredContent
        expect(update.state, JSON.stringify(update.failure)).toBe('succeeded')
        expect(update.final_value).toEqual({
          kind: 'value',
          value: expect.objectContaining({
            data: expect.objectContaining({ status: 'needs_sign_in' }),
          }),
        })
        expect(await instances.holder(run)).toBe(owner)
      })
    }),
  )
  expectNoUnavailable(requests)
})
