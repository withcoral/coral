import { expect, test } from 'vitest'
import { coralInstances, expectNoUnavailable } from '../../support/instances.ts'
import { managementClients } from '../../support/management.ts'
import {
  createWorkspace,
  observeRun,
  programRunner,
  type InputRequired,
} from '../../support/providers.ts'
import { withOwner } from '../../support/topology.ts'
import { addProviderProgram } from './waiting-run.ts'

test('An MCP client on 2026-07-28 approves through the Coral Instance that does not own the Run', async (context) => {
  const instances = coralInstances()
  const owner = 1
  const requests = await instances.during(() =>
    withOwner(context, async ({ page, step, target, accessToken }) => {
      const api = managementClients(accessToken)
      const workspace = await createWorkspace(accessToken, `Prompts ${crypto.randomUUID()}`)
      // No session: the proxy may send each call to either Coral Instance.
      const runner = programRunner(page, target, workspace, {
        protocolVersion: '2026-07-28',
        taskIntent: 'Add the weather provider',
      })
      const program = addProviderProgram()
      let asked!: InputRequired
      let key = ''
      let approval = ''
      let run = ''
      await step('exec on the second Coral Instance asks the client to approve', async () => {
        run = await instances.startOn(owner, async () => {
          asked = await runner.ask(program)
          ;[key] = Object.keys(asked.inputRequests)
          expect(key).toMatch(/^approval\//)
          approval = `workspaces/${workspace}/approvals/${key.slice('approval/'.length)}`
          return (await api.activity.inspectApproval({ name: approval })).run
        })
        expect(await instances.holder(run)).toBe(owner)
      })
      let answered!: Awaited<ReturnType<typeof runner.answer>>
      await step('Answer the prompt through the other Coral Instance', async () => {
        const sent = await instances.during(() =>
          instances.on(instances.other(owner), async () => {
            answered = await runner.answer(program, asked, { [key]: { action: 'accept' } })
          }),
        )
        expect(sent.filter(({ front }) => front === 'mcp')).toEqual([
          expect.objectContaining({ instance: instances.other(owner), status: 200 }),
        ])
        expect((await api.activity.inspectApproval({ name: approval })).state).toBe('approved')
      })
      await step('The owner records the approval, and the Program finishes', async () => {
        const { update } = await observeRun(runner, answered)
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
