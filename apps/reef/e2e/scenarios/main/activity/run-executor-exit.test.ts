import { expect, inject, test } from 'vitest'
import { descendantProcesses } from '../../../support/os-processes.ts'
import { observeRun } from '../../../support/providers.ts'
import { withOwner } from '../../../support/topology.ts'
import { pauseForApproval } from '../providers/program-approval.ts'

/** The local Run Executors that the Coral processes Reel started run now. */
async function localRunExecutors() {
  const coral = inject('reel').target.coralPids ?? []
  const children = (await Promise.all(coral.map((pid) => descendantProcesses(pid)))).flat()
  return children.filter((row) => /\brun-executor\b/.test(row.args)).map((row) => row.pid)
}

test('A Run whose local Run Executor dies ends interrupted at once, and the next Run starts on a new one', async (context) => {
  await withOwner(context, async ({ page, target, accessToken, step }) => {
    let started!: Awaited<ReturnType<typeof pauseForApproval>>
    await step('The Run waits for the owner to approve addProvider', async () => {
      started = await pauseForApproval(page, target, accessToken, 'Run Executor exit')
    })
    const { runner, paused } = started
    let killed: number[] = []
    await step('Kill the local Run Executor that runs it', async () => {
      killed = await localRunExecutors()
      expect(killed.length).toBeGreaterThan(0)
      for (const pid of killed) process.kill(pid, 'SIGKILL')
    })
    await step('The Run ends interrupted at once', async () => {
      const killedAt = Date.now()
      let update = paused
      for (let pages = 0; pages < 10 && update.state === 'running'; pages++)
        update = (await runner.wait(update)).structuredContent
      expect(update.state).toBe('interrupted')
      expect(Date.now() - killedAt).toBeLessThan(5_000)
    })
    await step('The next Run succeeds on a new local Run Executor', async () => {
      const { update } = await observeRun(runner, await runner.exec('return 1;'))
      expect(update.final_value).toEqual({ kind: 'value', value: 1 })
      const running = await localRunExecutors()
      expect(running.some((pid) => !killed.includes(pid))).toBe(true)
    })
  })
})
