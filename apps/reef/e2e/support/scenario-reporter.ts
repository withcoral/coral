import { mkdir } from 'node:fs/promises'
import { join, relative } from 'node:path'
import type { Reporter, TestCase, TestModule, TestSpecification } from 'vitest/node'

import {
  exec,
  json,
  renderRun,
  renderScenario,
  snapshotSource,
  type ScenarioResult,
} from './artifacts.ts'
import { bootstrapConfig, reefRoot, repoRoot } from './config.ts'
import { detectFfmpeg } from './ffmpeg.ts'

// Vitest owns the whole-test outcome, including assertions outside recordings,
// retries, and cleanup hooks. Browser sessions only own their own evidence.
export default class ScenarioReporter implements Reporter {
  private writing: Promise<void> = Promise.resolve()
  private completed = new Set<string>()

  async onTestRunStart(specifications: ReadonlyArray<TestSpecification>) {
    // This event precedes all project global setups. The command, not a target,
    // owns the artifact root and the optional-tool decision.
    if (!specifications.length) return
    const config = bootstrapConfig(process.env)
    const projects = [...new Set(specifications.map((specification) => specification.project))]
    const targets = projects.map((project) => (config.attached ? 'attached-local' : project.name))
    await mkdir(config.runDir, { recursive: true })
    const commit =
      config.source?.commit ??
      (await exec('git', ['rev-parse', 'HEAD'], { cwd: repoRoot })).stdout.trim()
    const dirty =
      config.source?.dirty ??
      Boolean((await exec('git', ['status', '--porcelain'], { cwd: repoRoot })).stdout.trim())
    await json(join(config.runDir, 'run.json'), {
      startedAt: new Date().toISOString(),
      commit,
      dirty,
      film: config.film,
      probes: config.probes,
      mode: config.attached ? 'attached' : 'managed',
      targets,
    })
    for (const name of targets) {
      await mkdir(join(config.runDir, name), { recursive: true })
      await json(join(config.runDir, name, 'target.json'), { name, state: 'waiting', pids: [] })
    }
    const converter = await detectFfmpeg(config.ffmpegBinary)
    if (converter.notice) console.warn(`Reel: ${converter.notice}`)
    for (const project of projects) {
      project.provide('reelBootstrap', {
        ...config,
        postgresURL: ['main-postgres', 'multi-instance'].includes(project.name)
          ? config.postgresURL
          : undefined,
        ffmpegBinary: converter.binary,
      })
    }
    await renderRun(config.runDir)
    console.log(`Reel artifacts: ${config.runDir}/index.html`)
  }

  onTestCaseReady(test: TestCase) {
    return this.enqueue(test, true)
  }

  onTestCaseResult(test: TestCase) {
    return this.enqueue(test, false)
  }

  onTestRunEnd(modules: ReadonlyArray<TestModule>) {
    // Reconcile the authoritative final collection, including tests failed or
    // skipped before their callbacks could create any browser evidence.
    for (const module of modules) {
      for (const test of module.children.allTests()) {
        if (test.result().state !== 'pending') void this.enqueue(test, false)
      }
    }
    return this.writing
  }

  private enqueue(test: TestCase, pending: boolean) {
    // Reporter callbacks can overlap. A slow initial render must never overwrite
    // a final result, nor race another test's run-index update.
    this.writing = this.writing.then(() => this.save(test, pending))
    return this.writing
  }

  private async save(test: TestCase, pending: boolean) {
    const config = test.project.getProvidedContext().reel
    if (!config) throw new Error('Reel reporter requires the project fixture')
    const dir = join(config.runDir, config.target.name, test.id)
    // Batched Vitest events can deliver ready after result for a fast test.
    if (pending && this.completed.has(dir)) return
    await mkdir(dir, { recursive: true })
    await snapshotSource(test.module.moduleId, join(dir, 'scenario.ts'))
    const outcome = test.result()
    const timing = test.diagnostic()
    const result: ScenarioResult = {
      id: test.id,
      scenario: test.fullName,
      source: relative(reefRoot, test.module.moduleId),
      target: config.target.name,
      status: pending ? 'pending' : outcome.state,
      startedAt: timing?.startTime ? new Date(timing.startTime).toISOString() : undefined,
      durationMs: pending ? undefined : timing?.duration,
      retries: timing?.retryCount ?? 0,
      repeats: timing?.repeatCount ?? 0,
      failure:
        outcome.state === 'failed'
          ? outcome.errors.map((error) => error.stack || error.message).join('\n\n')
          : undefined,
      note: outcome.state === 'skipped' ? outcome.note : undefined,
    }
    await renderScenario(dir, result)
    if (!pending) this.completed.add(dir)
    await renderRun(config.runDir)
  }
}
