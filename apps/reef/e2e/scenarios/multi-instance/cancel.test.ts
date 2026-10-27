import { expect, test } from 'vitest'
import { ActivityRunState } from '../../generated/coral/management/v1/activity_pb.ts'
import { coralInstances, expectNoUnavailable } from '../../support/instances.ts'
import { managementClients } from '../../support/management.ts'
import { withOwner } from '../../support/topology.ts'
import { runWaitingForApproval } from './waiting-run.ts'

test('Reef cancels through one Coral Instance a Run that the other owns, over its one connection', async (context) => {
  const instances = coralInstances()
  const owner = 1
  const requests = await instances.during(() =>
    withOwner(context, async ({ page, step, target, accessToken }) => {
      const api = managementClients(accessToken)
      let waiting!: Awaited<ReturnType<typeof runWaitingForApproval>>
      await step('A Run on the second Coral Instance waits for your approval', async () => {
        waiting = await runWaitingForApproval(page, target, accessToken, owner)
        expect(await instances.holder(waiting.run)).toBe(owner)
      })
      await step('Open the Run in Reef through the Coral Instance that owns it', async () => {
        await instances.on(owner, async () => {
          await page.goto(waiting.paused.required_actions[0].run_status_url)
          await page.getByRole('button', { name: 'Cancel run', exact: true }).waitFor()
        })
      })
      await step('Cancel it through the other Coral Instance', async () => {
        const sent = await instances.during(() =>
          instances.on(instances.other(owner), async () => {
            await page.getByRole('button', { name: 'Cancel run', exact: true }).click()
            await page
              .getByRole('dialog', { name: 'Cancel this run?' })
              .getByRole('button', { name: 'Cancel run', exact: true })
              .click()
            // The owner ends the Run within its five-second shutdown grace.
            await page
              .getByRole('dialog', { name: 'Activity details' })
              .locator('header')
              .getByText('canceled', { exact: true })
              .waitFor({ timeout: 15_000 })
          }),
        )
        const cancel = sent.filter(
          ({ path }) => path === '/coral.management.v1.ActivityService/CancelActivityRun',
        )
        expect(cancel).toEqual([
          expect.objectContaining({ instance: instances.other(owner), grpcStatus: '0' }),
        ])
        // Reef's one connection carried requests to both Coral Instances.
        const reef = (await instances.requests()).filter(
          ({ front, connection }) => front === 'grpc' && connection === cancel[0].connection,
        )
        expect(new Set(reef.map(({ instance }) => instance))).toEqual(new Set([0, 1]))
      })
      await step('The owner records the cancellation', async () => {
        const { run } = await api.activity.inspectRun({ name: waiting.run, pageSize: 1 })
        expect(run?.state).toBe(ActivityRunState.CANCELED)
        expect((await waiting.runner.wait(waiting.paused)).structuredContent.state).toBe('canceled')
        expect(await instances.holder(waiting.run)).toBe(owner)
      })
    }),
  )
  expectNoUnavailable(requests)
})
