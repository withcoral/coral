import { execFile } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import { readdir, readFile, realpath, rename, rm, writeFile } from 'node:fs/promises'
import { basename, dirname, join, relative, resolve, sep } from 'node:path'
import { promisify } from 'node:util'

import type {
  Result,
  RunSummary,
  ScenarioEntry,
  ScenarioResult,
  SessionResult,
} from '../viewer/types.ts'
import { reefRoot } from './config.ts'
import { viewerRenderer } from './viewer-renderer.ts'
import type { StaticDocument } from '../../scripts/static-document.ts'

export type { Result, ScenarioResult } from '../viewer/types.ts'
export const exec = promisify(execFile)

/**
 * Saves a scenario's source: its entry file, then each shared definition it
 * imports from `scenarios/`, so evidence shows the journey a parity entry ran.
 */
export async function snapshotSource(entry: string, destination: string) {
  const source = await readFile(entry, 'utf8')
  const scenarios = join(reefRoot, 'e2e/scenarios') + sep
  const definitions = [...source.matchAll(/from '(\.{1,2}\/[^']+\.ts)'/g)]
    .map((match) => resolve(dirname(entry), match[1]))
    .filter((path) => path.startsWith(scenarios) && !path.endsWith('.test.ts'))
  const parts = [source]
  for (const path of definitions)
    parts.push(
      `\n// Shared definition: ${relative(reefRoot, path)}\n\n${await readFile(path, 'utf8')}`,
    )
  await writeFile(destination, parts.join(''))
}

export function slug(value: string) {
  return (
    value
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/^-|-$/g, '')
      .slice(0, 80) || 'session'
  )
}

export function escape(value: string) {
  return value.replace(
    /[&<>"']/g,
    (character) =>
      ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[character]!,
  )
}

export async function json(path: string, value: unknown) {
  await publish(path, `${JSON.stringify(value, null, 2)}\n`)
}

async function publish(path: string, content: string) {
  // Readers can observe a page while another worker finalizes it.
  const temporary = `${path}.${randomUUID()}.tmp`
  try {
    await writeFile(temporary, content)
    await rename(temporary, path)
  } finally {
    await rm(temporary, { force: true })
  }
}

// Metadata/source reads obey the same containment rule as artifact downloads.
async function readEvidence(dir: string, file: string) {
  const base = await realpath(dir)
  const path = await realpath(join(base, file))
  if (!path.startsWith(base + sep)) throw new Error('Outside evidence directory')
  return readFile(path, 'utf8')
}

export async function renderSession(dir: string, result: Result) {
  await json(join(dir, 'result.json'), result)
  let source: string | undefined
  let hasFailureScreenshot = false
  try {
    source = await readEvidence(dir, 'scenario.ts')
  } catch {
    // A partial recording may have no source snapshot.
  }
  if (result.failure) {
    try {
      const base = await realpath(dir)
      hasFailureScreenshot = (await realpath(join(base, 'failure.png'))).startsWith(base + sep)
    } catch {
      // Failure screenshots are best-effort; do not offer a broken link.
    }
  }
  const { render } = await viewerRenderer()
  await writePage(dir, render.session({ result, source, hasFailureScreenshot }))
}

export async function collectScenarios(runDir: string) {
  const results: ScenarioEntry[] = []
  for (const target of await readdir(runDir, { withFileTypes: true })) {
    if (!target.isDirectory()) continue
    for (const scenario of await readdir(join(runDir, target.name), { withFileTypes: true })) {
      if (!scenario.isDirectory()) continue
      const path = `${target.name}/${scenario.name}`
      try {
        const result: ScenarioResult = JSON.parse(
          await readEvidence(join(runDir, path), 'scenario.json'),
        )
        if (
          !result ||
          result.id !== scenario.name ||
          typeof result.scenario !== 'string' ||
          result.target !== target.name ||
          !['pending', 'passed', 'failed', 'skipped'].includes(result.status) ||
          (result.durationMs !== undefined && !Number.isFinite(result.durationMs))
        )
          throw new Error('Invalid scenario metadata')
        results.push({ path, result })
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error
      }
    }
  }
  return results
}

export async function collectSessions(scenarioDir: string) {
  const results: SessionResult[] = []
  let entries
  try {
    const base = await realpath(scenarioDir)
    const sessions = await realpath(join(base, 'sessions'))
    if (!sessions.startsWith(base + sep)) throw new Error('Outside scenario directory')
    entries = await readdir(sessions, { withFileTypes: true })
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return results
    throw error
  }
  for (const entry of entries) {
    if (!entry.isDirectory()) continue
    const path = `sessions/${entry.name}`
    try {
      const result: Result = JSON.parse(await readEvidence(join(scenarioDir, path), 'result.json'))
      if (
        !result ||
        result.id !== entry.name ||
        result.scenarioId !== basename(scenarioDir) ||
        typeof result.label !== 'string' ||
        (result.actor !== undefined && typeof result.actor !== 'string') ||
        !['setup', 'interaction'].includes(result.purpose) ||
        !Number.isInteger(result.sequence) ||
        result.sequence < 1 ||
        !Number.isInteger(result.attempt) ||
        result.attempt < 1 ||
        !['passed', 'failed', 'skipped'].includes(result.status) ||
        !Number.isFinite(result.durationMs)
      )
        throw new Error('Invalid recorded session metadata')
      results.push({ path, result })
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error
    }
  }
  return results.toSorted((a, b) => a.result.sequence - b.result.sequence)
}

// Flattening is for playback verification only; navigation is scenario-owned.
export async function collectResults(runDir: string) {
  const results: SessionResult[] = []
  for (const scenario of await collectScenarios(runDir)) {
    for (const session of await collectSessions(join(runDir, scenario.path))) {
      results.push({ path: `${scenario.path}/${session.path}`, result: session.result })
    }
  }
  return results
}

export async function renderScenario(dir: string, result: ScenarioResult) {
  await json(join(dir, 'scenario.json'), result)
  const sessions = await collectSessions(dir)
  let source: string | undefined
  try {
    source = await readEvidence(dir, 'scenario.ts')
  } catch {
    // Partial setup can lack source.
  }
  const { render } = await viewerRenderer()
  await writePage(dir, render.scenario({ result, sessions, source }))
}

async function summarizeRun(dir: string): Promise<RunSummary> {
  const run: RunSummary = {
    id: basename(dir),
    dirty: false,
    film: false,
    probes: false,
    targets: [],
    results: [],
  }
  try {
    const metadata = JSON.parse(await readEvidence(dir, 'run.json'))
    run.startedAt = typeof metadata.startedAt === 'string' ? metadata.startedAt : undefined
    run.commit = typeof metadata.commit === 'string' ? metadata.commit : undefined
    if (
      !Array.isArray(metadata.targets) ||
      metadata.targets.some(
        (name: unknown) =>
          typeof name !== 'string' ||
          !name ||
          name === '.' ||
          name === '..' ||
          /[/\\\\]/.test(name),
      ) ||
      new Set(metadata.targets).size !== metadata.targets.length
    )
      throw new Error('Invalid run targets')
    run.targets = metadata.targets.map((name: string) => ({ name, state: 'waiting' }))
    run.dirty = metadata.dirty === true
    run.film = metadata.film === true
    run.probes = metadata.probes === true
  } catch {
    run.warning = 'Run metadata unavailable. This run may be unfinished or damaged.'
  }
  try {
    run.results = await collectScenarios(dir)
  } catch {
    run.warning = 'Scenario metadata unavailable. This run may be unfinished or damaged.'
  }
  const failures: string[] = []
  for (const target of run.targets) {
    try {
      const metadata = JSON.parse(await readEvidence(dir, `${target.name}/target.json`))
      if (
        metadata.name !== target.name ||
        !['waiting', 'starting', 'ready', 'closed', 'failed'].includes(metadata.state)
      )
        throw new Error('Invalid target metadata')
      target.state = metadata.state
      if (typeof metadata.error === 'string') failures.push(`${target.name}: ${metadata.error}`)
    } catch {
      target.state = 'unavailable'
      failures.push(`${target.name}: target metadata unavailable`)
    }
  }
  run.setupError = failures.join('\n\n')
  return run
}

export async function renderLanding(root: string) {
  const entries = (await readdir(root, { withFileTypes: true })).filter((entry) =>
    entry.isDirectory(),
  )
  const runs = await Promise.all(entries.map((entry) => summarizeRun(join(root, entry.name))))
  const { render } = await viewerRenderer()
  return render.landing(runs)
}

export async function renderRun(runDir: string) {
  const run = await summarizeRun(runDir)
  const { render } = await viewerRenderer()
  await writePage(runDir, render.run(run))
}

/** Regenerate presentation from saved metadata/source without rerunning tests. */
export async function refreshRunPages(runDir: string) {
  for (const scenario of await collectScenarios(runDir)) {
    const directory = join(runDir, scenario.path)
    for (const session of await collectSessions(directory)) {
      await renderSession(join(directory, session.path), session.result)
    }
    await renderScenario(directory, scenario.result)
  }
  await renderRun(runDir)
}

async function writePage(dir: string, page: StaticDocument) {
  // Publish styles before HTML. The content-addressed name keeps old HTML valid
  // while another worker or a newer build refreshes the page.
  await publish(join(dir, page.stylesheet), page.css)
  await publish(join(dir, 'index.html'), page.html)
}
