import { expect, test } from 'vitest'
import { withBrowserSession } from '../support/browser-session.ts'

// Real runner probes for the scenario/reporting seam, including paths with no
// browser and failures after every browser has closed.
test('Scenario ownership: nested sessions and cleanup failure', async (context) => {
  await withBrowserSession(
    context,
    async ({ page, step }) => {
      await step('Open Alice’s first session', async () => {
        await page.goto('/')
        await page.getByRole('navigation', { name: 'Coral', exact: true }).waitFor()
      })
      await withBrowserSession(
        context,
        async ({ page: second, step: secondStep }) => {
          await secondStep('Open Alice’s second session', async () => {
            await second.goto('/')
            await second.getByRole('navigation', { name: 'Coral', exact: true }).waitFor()
          })
        },
        { actor: 'Alice', label: 'Login', purpose: 'setup' },
      )
    },
    { actor: 'Alice', label: 'Login', purpose: 'setup' },
  )
  context.onTestFinished(() => {
    throw new Error('SCENARIO_CLEANUP_FAILURE')
  })
})

test('Scenario ownership: failure before recording', () => {
  throw new Error('SCENARIO_BEFORE_RECORDING')
})

test.skip('Scenario ownership: skipped without recording', () => {})

test('Scenario ownership: retry retains recordings', { retry: 1 }, async (context) => {
  await withBrowserSession(context, async ({ page, step }) => {
    await step('Open Reef', async () => {
      await page.goto('/')
      await page.getByRole('navigation', { name: 'Coral', exact: true }).waitFor()
    })
  })
  expect(context.task.result?.retryCount, 'SCENARIO_RETRY').toBe(1)
})

for (let index = 0; index < 2; index++) {
  test('Scenario ownership: identical display name', () => {})
}
