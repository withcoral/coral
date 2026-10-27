import { expect, test } from 'vitest'
import { coralInstances, expectNoUnavailable } from '../../support/instances.ts'
import { managementClients } from '../../support/management.ts'
import { observeRun } from '../../support/providers.ts'
import { withOwner } from '../../support/topology.ts'
import { openReview } from '../main/providers/program-approval.ts'
import { runWaitingForApproval } from './waiting-run.ts'

test('Reef approves a call on a Run that the other Coral Instance owns', async (context) => {
  const instances = coralInstances()
  const owner = 0
  const requests = await instances.during(() =>
    withOwner(context, async ({ page, step, target, accessToken }) => {
      const api = managementClients(accessToken)
      let waiting!: Awaited<ReturnType<typeof runWaitingForApproval>>
      await step('A Run on the first Coral Instance waits for your approval', async () => {
        waiting = await runWaitingForApproval(page, target, accessToken, owner)
        expect(await instances.holder(waiting.run)).toBe(owner)
      })
      await step('Approve the call in Reef through the other Coral Instance', async () => {
        const sent = await instances.during(() =>
          instances.on(instances.other(owner), async () => {
            await openReview(page, waiting.paused, 'addProvider')
            await page.getByRole('button', { name: 'Approve this call', exact: true }).click()
            await page.getByText('Approved', { exact: true }).waitFor()
          }),
        )
        expect(
          sent.filter(({ path }) => path === '/coral.management.v1.ActivityService/DecideApproval'),
        ).toEqual([expect.objectContaining({ instance: instances.other(owner), grpcStatus: '0' })])
      })
      await step('The owner records the approval, and the Program continues', async () => {
        expect((await api.activity.inspectApproval({ name: waiting.approval })).state).toBe(
          'approved',
        )
        const { update } = await observeRun(
          waiting.runner,
          await waiting.runner.wait(waiting.paused),
        )
        expect(update.final_value).toEqual({
          kind: 'value',
          value: expect.objectContaining({
            data: expect.objectContaining({ status: 'needs_sign_in' }),
          }),
        })
        expect(await instances.holder(waiting.run)).toBe(owner)
      })
    }),
  )
  expectNoUnavailable(requests)
})
