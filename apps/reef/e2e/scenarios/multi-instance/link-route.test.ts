import { expect, test } from 'vitest'
import { coralInstances, expectNoUnavailable } from '../../support/instances.ts'
import { managementClients } from '../../support/management.ts'
import { observeRun, openReview } from '../../support/providers.ts'
import { withOwner } from '../../support/topology.ts'
import { runWaitingForApproval } from './waiting-run.ts'

test('An MCP client on 2026-07-28 is handed the link, decides in Reef, and waits through the Coral Instance that does not own the Run', async (context) => {
  const instances = coralInstances()
  const owner = 1
  const requests = await instances.during(() =>
    withOwner(context, async ({ page, step, target, accessToken }) => {
      const api = managementClients(accessToken)
      let waiting!: Awaited<ReturnType<typeof runWaitingForApproval>>
      await step(
        'exec on the second Coral Instance returns the required action, never a prompt',
        async () => {
          waiting = await runWaitingForApproval(page, target, accessToken, owner, '2026-07-28')
          const [action] = waiting.paused.required_actions
          expect(action.url).toContain(`/${waiting.approval}`)
          expect(action.tool).toBe('coral.addProvider')
          expect(action.provider).toEqual(expect.any(String))
          expect(await instances.holder(waiting.run)).toBe(owner)
        },
      )
      await step('Approve the call on the Approvals Page', async () => {
        const review = await openReview(page, waiting.paused, 'addProvider')
        await review.getByRole('button', { name: 'Approve', exact: true }).click()
        await review.getByText('Approved', { exact: true }).waitFor()
        expect((await api.activity.inspectApproval({ name: waiting.approval })).state).toBe(
          'approved',
        )
      })
      await step('wait through the other Coral Instance reports the Run going on', async () => {
        let observed!: Awaited<ReturnType<typeof waiting.runner.wait>>
        const sent = await instances.during(() =>
          instances.on(instances.other(owner), async () => {
            observed = await waiting.runner.wait(waiting.paused)
          }),
        )
        expect(sent.filter(({ front }) => front === 'mcp')).toEqual([
          expect.objectContaining({
            instance: instances.other(owner),
            status: 200,
          }),
        ])
        expect(observed.structuredContent.required_actions).toEqual([])
        const { update } = await observeRun(waiting.runner, observed)
        expect(update.final_value).toEqual({
          kind: 'value',
          value: expect.objectContaining({
            data: expect.objectContaining({ status: 'needs_person' }),
          }),
        })
        expect(await instances.holder(waiting.run)).toBe(owner)
      })
    }),
  )
  expectNoUnavailable(requests)
})
