import assert from 'node:assert/strict'
import { spawn, spawnSync } from 'node:child_process'
import { readFile, stat } from 'node:fs/promises'
import { basename, join } from 'node:path'
import { chromium } from 'playwright'

import { collectResults, collectScenarios, collectSessions } from '../support/artifacts.ts'
import { reefRoot, runsRoot } from '../support/config.ts'
import { resultsServer } from '../support/results-server.ts'
import { bounded } from '../support/stack.ts'

const support = spawnSync(
  process.execPath,
  [
    '--test',
    'e2e/support/harness.test.ts',
    'e2e/support/viewer.test.ts',
    'e2e/support/run.test.ts',
    'e2e/scripts/container.test.ts',
    'e2e/support/desktop.test.ts',
    'e2e/support/os-processes.test.ts',
    '../../scripts/postgres.test.mjs',
    'e2e/support/data-responses.test.ts',
    'e2e/support/update-bytes.test.ts',
    'e2e/support/round-robin-proxy.test.ts',
  ],
  {
    cwd: reefRoot,
    stdio: 'inherit',
  },
)
if (support.error) throw support.error
assert.equal(support.status, 0, 'Harness safety tests must pass')

// Run intentionally failing tests in a separate Vitest process, then assert on
// its real output/evidence. No expected-failure tests pollute the product suite.
const child = spawn(process.execPath, ['e2e/scripts/run.ts'], {
  cwd: reefRoot,
  env: {
    ...process.env,
    E2E_HARNESS_PROBES: '1',
    // Container self-check evidence has its own final host-visible directory.
    E2E_RUN_ID: process.env.E2E_SELF_CHECK_RUN_ID,
    E2E_FILM: '0',
    E2E_BASE_URL: '',
    E2E_ALLOW_ATTACHED: '',
    E2E_FFMPEG_BINARY: join(reefRoot, 'e2e/probes/intentionally-missing-ffmpeg'),
  },
  stdio: ['ignore', 'pipe', 'pipe'],
})
let output = ''
for (const stream of [child.stdout, child.stderr]) {
  stream.setEncoding('utf8').on('data', (text: string) => {
    output += text
    process.stdout.write(text)
  })
}
const status = await new Promise<number | null>((resolve, reject) => {
  child.on('error', reject)
  child.on('close', resolve)
})
assert.equal(status, 1, 'The deliberate failures must fail the child test command')
assert.match(output, /REEL_ORIGINAL_ASSERTION/)
assert.equal(
  (output.match(/ffmpeg not found; keeping WebM recordings\./g) ?? []).length,
  1,
  'Missing ffmpeg should produce one run-level notice',
)
assert.doesNotMatch(output, /Optional MP4 conversion \(WebM retained\): Error: spawn/)
const reportedRuns = [...output.matchAll(/Reel artifacts: (.+)\/index.html/g)]
assert.equal(reportedRuns.length, 1, 'A test command must publish one viewer run')
const runDir = reportedRuns[0][1]
assert.ok(runDir, 'Run bootstrap must report its collision-safe artifact directory')
const results = await collectResults(runDir)
assert.equal(results.length, 8)
const scenarios = await collectScenarios(runDir)
assert.equal(scenarios.length, 10, 'The table must count tests, not recordings')
assert.ok(
  scenarios.every(({ result }) => result.status !== 'pending'),
  'Completed tests cannot regress to pending',
)
const scenarioNamed = (name: string) => {
  const found = scenarios.find(({ result }) => result.scenario === name)
  assert.ok(found, name)
  return found
}
for (const [name, expectedStatus, error] of [
  ['Harness probe: after-session', 'failed', 'REEL_LATE_ASSERTION'],
  ['Scenario ownership: nested sessions and cleanup failure', 'failed', 'SCENARIO_CLEANUP_FAILURE'],
  ['Scenario ownership: failure before recording', 'failed', 'SCENARIO_BEFORE_RECORDING'],
  ['Scenario ownership: skipped without recording', 'skipped', undefined],
  ['Scenario ownership: retry retains recordings', 'passed', undefined],
] as const) {
  const { result } = scenarioNamed(name)
  assert.equal(result.status, expectedStatus, name)
  if (error) assert.ok(result.failure?.includes(error), name)
}
const nested = scenarioNamed('Scenario ownership: nested sessions and cleanup failure')
const children = await collectSessions(join(runDir, nested.path))
assert.equal(children.length, 2)
assert.equal(new Set(children.map(({ result }) => result.id)).size, 2)
assert.deepEqual(
  children.map(({ result }) => [result.actor, result.label, result.sequence, result.status]),
  [
    ['Alice', 'Login', 1, 'passed'],
    ['Alice', 'Login', 2, 'passed'],
  ],
)
assert.ok(nested.result.durationMs! >= Math.max(...children.map(({ result }) => result.durationMs)))
const retry = scenarioNamed('Scenario ownership: retry retains recordings')
assert.equal(retry.result.retries, 1)
assert.deepEqual(
  (await collectSessions(join(runDir, retry.path))).map(({ result }) => result.attempt),
  [1, 2],
)
const duplicates = scenarios.filter(
  ({ result }) => result.scenario === 'Scenario ownership: identical display name',
)
assert.equal(duplicates.length, 2)
assert.equal(new Set(duplicates.map(({ result }) => result.id)).size, 2)
for (const name of [
  'Scenario ownership: failure before recording',
  'Scenario ownership: skipped without recording',
]) {
  assert.equal((await collectSessions(join(runDir, scenarioNamed(name).path))).length, 0)
}
const server = await resultsServer(runsRoot)
const origin = `http://127.0.0.1:${(server.address() as { port: number }).port}`
let browser
try {
  browser = await chromium.launch({ timeout: 15_000 })
  const page = await browser.newPage()
  for (const { path, result } of results) {
    const passing = !['Harness probe: assertion', 'Harness probe: timeout'].includes(
      result.scenario,
    )
    assert.equal(result.status, passing ? 'passed' : 'failed', result.scenario)
    assert.equal(result.browserClosed, true, 'The recorded browser must actually disconnect')
    if (result.scenario.endsWith('assertion'))
      assert.match(result.failure!, /REEL_ORIGINAL_ASSERTION/)
    if (result.scenario.endsWith('timeout')) assert.match(result.failure!, /timed out/i)
    assert.equal(result.recording, 'session.webm', 'Unavailable ffmpeg must preserve WebM')
    assert.ok(
      !result.warnings.some((warning) => warning.includes('Optional MP4 conversion')),
      'Known-unavailable ffmpeg should not be retried for each recording',
    )
    assert.equal(result.trace, 'trace.zip')
    assert.equal(result.steps.length, 1)
    for (const file of [
      result.recording,
      result.trace,
      result.steps[0].screenshot,
      'scenario.ts',
      ...(passing ? [] : ['failure.png']),
    ]) {
      assert.ok((await stat(join(runDir, path, file))).size > 0, file)
    }
    await page.goto(`${origin}/${basename(runDir)}/${path}/index.html`)
    // Browsers defer offscreen muted autoplay (a long failure can push the video
    // below the fold). Once visible, it must play without a click or play() call.
    const video = page.locator('video')
    await video.scrollIntoViewIfNeeded()
    await bounded(
      video.evaluate(async (element: HTMLVideoElement) => {
        if (!element.autoplay || !element.muted || !element.playsInline)
          throw new Error('Recording must autoplay muted and inline')
        await new Promise<void>((resolve, reject) => {
          if (element.currentTime > 0) return resolve()
          element.addEventListener('timeupdate', () => {
            if (element.currentTime > 0) resolve()
          })
          element.addEventListener('error', () => reject(new Error('Video decoding failed')), {
            once: true,
          })
        })
        if (element.videoWidth !== 1280 || element.videoHeight !== 800)
          throw new Error('Unexpected recording dimensions')
        element.pause()
      }),
      15_000,
      'Recording playback',
    )
    const range: Response = await fetch(
      `${origin}/${basename(runDir)}/${path}/${result.recording}`,
      { headers: { Range: 'bytes=0-31' } },
    )
    assert.equal(range.status, 206)
    assert.equal((await range.arrayBuffer()).byteLength, 32)
  }
  const metadata = JSON.parse(await readFile(join(runDir, 'run.json'), 'utf8'))
  for (const name of metadata.targets) {
    const target = JSON.parse(await readFile(join(runDir, name, 'target.json'), 'utf8'))
    assert.equal(target.state, 'closed')
    for (const log of ['coral.log', 'reef.log']) {
      assert.ok((await stat(join(runDir, name, log))).isFile(), 'Process logs must be target-owned')
    }
    for (const pid of target.pids) {
      assert.throws(
        () => process.kill(pid, 0),
        { code: 'ESRCH' },
        'Managed app processes must exit',
      )
    }
  }
  console.log(
    `Harness self-check passed: scenario outcomes, nested recordings, retries, skips, late failures, autoplay and process cleanup.\n${runDir}/index.html`,
  )
} finally {
  await browser?.close()
  server.closeAllConnections()
  await new Promise<void>((resolve, reject) =>
    server.close((error) => (error ? reject(error) : resolve())),
  )
}
