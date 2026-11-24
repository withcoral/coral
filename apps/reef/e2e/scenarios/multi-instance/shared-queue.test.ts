import { expect, test } from 'vitest'
import { ActivityRunState } from '../../generated/coral/management/v1/activity_pb.ts'
import { coralInstances, expectNoUnavailable } from '../../support/instances.ts'
import { managementClients } from '../../support/management.ts'
import { observeRun, programRunner } from '../../support/providers.ts'
import { withOwner } from '../../support/topology.ts'
import { endAfterTest, runWaitingForApproval } from './waiting-run.ts'

test('A Run accepted through one Coral Instance waits in the shared queue and runs on the Run Executor of the other', async (context) => {
  const instances = coralInstances()
  const requests = await instances.during(() =>
    withOwner(context, async ({ page, step, target, accessToken }) => {
      const api = managementClients(accessToken)
      let waiting!: Awaited<ReturnType<typeof runWaitingForApproval>>
      let full!: number
      await step(
        'A Run that waits for your approval fills the one Run Executor of its Coral Instance',
        async () => {
          waiting = await runWaitingForApproval(page, target, accessToken, 0)
          full = waiting.owner
        },
      )
      await step(
        'A Run accepted through that Coral Instance runs on the Run Executor of the other',
        async () => {
          const runner = programRunner(page, target, waiting.workspace)
          let started!: Awaited<ReturnType<typeof runner.exec>>
          const run = await instances.startOn(full, async () => {
            started = await runner.exec('return 40 + 2;')
            const name = `workspaces/${waiting.workspace}/runs/${started.structuredContent.run_id}`
            endAfterTest(accessToken, name)
            return name
          })
          const { update } = await observeRun(runner, started)
          expect(update.final_value).toEqual({ kind: 'value', value: 42 })
          expect(await instances.holder(run)).toBe(instances.other(full))
        },
      )
      await step('Cancel the waiting Run, which stays on its Coral Instance', async () => {
        await api.activity.cancelActivityRun({ name: waiting.run })
        await expect
          .poll(
            async () =>
              (await api.activity.inspectRun({ name: waiting.run, pageSize: 1 })).run?.state,
          )
          .toBe(ActivityRunState.CANCELED)
        expect(await instances.holder(waiting.run)).toBe(full)
      })
    }),
  )
  expectNoUnavailable(requests)
})
