import assert from 'node:assert/strict'
import { mkdtemp, mkdir, readFile, rm, symlink, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { test } from 'node:test'
import { chromium } from 'playwright'

import {
  json,
  renderRun,
  renderScenario,
  renderSession,
  type Result,
  type ScenarioResult,
} from './artifacts.ts'
import { resultsServer } from './results-server.ts'
import { playbackScriptHash } from './playback.ts'

const result: Result = {
  id: 'session-1',
  scenarioId: 'scenario-1',
  label: 'Reef interaction',
  purpose: 'interaction',
  sequence: 1,
  attempt: 1,
  scenario: 'Cancel Workspace archival',
  source: 'e2e/scenarios/workspaces.test.ts',
  target: 'local-sqlite',
  status: 'passed',
  startedAt: '2026-09-08T10:00:00.000Z',
  durationMs: 4200,
  steps: [{ label: 'Workspace remains active', screenshot: '00-active.png', elapsedMs: 3000 }],
  recording: 'session.webm',
  trace: 'trace.zip',
  warnings: [],
  browserClosed: true,
}

const scenario: ScenarioResult = {
  id: result.scenarioId,
  scenario: result.scenario,
  source: result.source,
  target: result.target,
  status: 'passed',
  startedAt: result.startedAt,
  durationMs: 14200,
  retries: 0,
  repeats: 0,
}

async function fixture() {
  const root = await mkdtemp(join(tmpdir(), 'reel-review-'))
  async function run(id: string, startedAt: string, probes = false) {
    const runDir = join(root, id)
    const scenarioDir = join(runDir, 'local-sqlite', scenario.id)
    const sessionDir = join(scenarioDir, 'sessions', result.id)
    await mkdir(sessionDir, { recursive: true })
    await json(join(runDir, 'run.json'), {
      startedAt,
      commit: '123456789abcdef',
      dirty: true,
      film: true,
      probes,
      targets: ['local-sqlite'],
    })
    await json(join(runDir, 'local-sqlite', 'target.json'), {
      name: 'local-sqlite',
      state: 'closed',
      pids: [],
    })
    await writeFile(
      join(sessionDir, 'scenario.ts'),
      'test("cancel", () => expect(active).toBe(true))',
    )
    await writeFile(
      join(scenarioDir, 'scenario.ts'),
      'test("cancel", () => expect(active).toBe(true))',
    )
    await renderSession(sessionDir, result)
    await renderScenario(scenarioDir, scenario)
    await renderRun(runDir)
    return sessionDir
  }
  const server = await resultsServer(root)
  const origin = `http://127.0.0.1:${(server.address() as { port: number }).port}`
  return {
    root,
    origin,
    run,
    async close() {
      server.closeAllConnections()
      await new Promise<void>((resolve, reject) =>
        server.close((error) => (error ? reject(error) : resolve())),
      )
      await rm(root, { recursive: true, force: true })
    },
  }
}

test('landing page shows the newest scenario run, with history and self-checks separate', async () => {
  const f = await fixture()
  try {
    await f.run('older', '2026-09-07T10:00:00Z')
    await f.run('latest', '2026-09-08T10:00:00Z')
    await f.run('newest-probe', '2026-09-08T11:00:00Z', true)
    const response = await fetch(f.origin)
    const html = await response.text()
    assert.match(html, /Latest scenario run/)
    assert.match(html, /Cancel Workspace archival/)
    assert.match(html, /href="\/latest\/local-sqlite\/scenario-1\/index.html"/)
    assert.match(html, /14\.2s/)
    assert.match(html, /1234567/)
    assert.match(html, /Uncommitted changes/)
    assert.match(html, /Film mode/)
    assert.match(html, /Run history/)
    assert.match(html, /Harness self-checks/)
    const latest = html.slice(html.indexOf('Latest scenario run'), html.indexOf('Run history'))
    assert.doesNotMatch(latest, /newest-probe|href="\/older\//)
    assert.match(response.headers.get('content-security-policy')!, /default-src 'none'/)
    assert.doesNotMatch(html, /<script|<style\b|\sstyle="/)
    assert.doesNotMatch(response.headers.get('content-security-policy')!, /unsafe-inline/)
  } finally {
    await f.close()
  }
})

test('scenario results follow source folders without merging repeated names or hiding outcomes', async () => {
  const f = await fixture()
  const browser = await chromium.launch()
  try {
    await f.run('folders', result.startedAt)
    const runDir = join(f.root, 'folders')
    const entries = [
      {
        id: 'account',
        source: 'e2e/scenarios/main/accounts/authority.test.ts',
        scenario: 'Account access',
      },
      {
        id: 'provider',
        source: 'e2e/scenarios/main/providers/lifecycle.test.ts',
        scenario:
          'Provider lifecycle preserves configuration, accounts, tool policy, and retained history',
      },
      {
        id: 'setup-passed',
        source: 'e2e/scenarios/main/providers/setup/configuration.test.ts',
        scenario: 'Repeated name',
      },
      {
        id: 'setup-failed',
        source: 'e2e/scenarios/main/providers/setup/oauth.test.ts',
        scenario: 'Repeated name',
        status: 'failed',
      },
      {
        id: 'setup-pending',
        source: 'e2e/scenarios/main/providers/setup/discovery.test.ts',
        scenario: 'Discovery',
        status: 'pending',
        durationMs: undefined,
      },
      {
        id: 'workspace',
        source: 'e2e\\scenarios\\main\\workspaces\\membership.test.ts',
        scenario: 'Workspace access',
        status: 'skipped',
      },
      {
        id: 'unsafe',
        source: 'e2e/scenarios/main/<unsafe>/source.test.ts',
        scenario: 'Escaped folder',
      },
    ] satisfies Partial<ScenarioResult>[]
    for (const entry of entries) {
      const dir = join(runDir, scenario.target, entry.id)
      await mkdir(dir)
      await renderScenario(dir, { ...scenario, ...entry })
    }
    await renderRun(runDir)
    for (const width of [1280, 390]) {
      const page = await browser.newPage({
        viewport: { width, height: 900 },
        javaScriptEnabled: false,
        colorScheme: width === 390 ? 'dark' : 'light',
      })
      for (const url of [f.origin, `${f.origin}/folders/index.html`]) {
        await page.goto(url)
        const table = page.getByRole('table', { name: 'Scenario results' })
        const accounts = table.getByRole('rowgroup', { name: 'main/accounts', exact: true })
        const providers = table.getByRole('rowgroup', { name: 'main/providers', exact: true })
        const setup = table.getByRole('rowgroup', { name: 'main/providers/setup', exact: true })
        const workspaces = table.getByRole('rowgroup', { name: 'main/workspaces', exact: true })
        await setup.waitFor()
        assert.equal(await table.getByTestId('scenario-link').count(), entries.length + 1)
        assert.deepEqual(await accounts.getByTestId('scenario-link').allTextContents(), [
          'Account access',
        ])
        assert.deepEqual(await providers.getByTestId('scenario-link').allTextContents(), [
          entries[1].scenario,
        ])
        assert.deepEqual(await setup.getByTestId('scenario-link').allTextContents(), [
          'Repeated name',
          'Discovery',
          'Repeated name',
        ])
        assert.deepEqual(await workspaces.getByTestId('scenario-link').allTextContents(), [
          'Workspace access',
        ])
        const links = await setup
          .getByTestId('scenario-link')
          .evaluateAll((elements) => elements.map((element) => element.getAttribute('href')))
        assert.equal(new Set(links).size, 3, 'same-named scenarios retain distinct evidence links')
        assert.ok(links[0]!.includes('/setup-failed/'), 'failures lead within their folder')
        assert.match(await setup.innerText(), /1 failed/)
        assert.match(await setup.innerText(), /1 incomplete/)
        assert.match(await workspaces.innerText(), /Skipped/)
        const providerLabel = await providers.getByText('providers/', { exact: true }).boundingBox()
        const setupLabel = await setup.getByText('setup/', { exact: true }).boundingBox()
        assert.ok(setupLabel!.x > providerLabel!.x, 'nested folders are visibly indented')
        assert.equal(await table.locator('unsafe').count(), 0)
        assert.equal(
          await table.getByRole('rowgroup', { name: 'main/<unsafe>', exact: true }).count(),
          1,
        )
        assert.equal(await page.locator('script').count(), 0)
        assert.equal(
          await table
            .getByTestId('scenario-link')
            .evaluateAll((elements) =>
              elements.some(
                (link) =>
                  link.getBoundingClientRect().right >
                  link.closest('[role="cell"]')!.getBoundingClientRect().right + 1,
              ),
            ),
          false,
          'indented scenario names wrap instead of covering their outcomes',
        )
        assert.equal(
          await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth),
          true,
        )
        await setup.getByTestId('scenario-link').first().click()
        await page.getByRole('heading', { name: 'Repeated name', exact: true }).waitFor()
      }
      await page.close()
    }
  } finally {
    await browser.close()
    await f.close()
  }
})

test('one scenario owns ordered actor recordings, repeated logins, retries and the final outcome', async () => {
  const f = await fixture()
  try {
    await f.run('grouped', result.startedAt)
    const dir = join(f.root, 'grouped', scenario.target, scenario.id)
    for (const [id, sequence, attempt] of [
      ['z-login', 2, 1],
      ['a-login', 3, 1],
      ['retry-login', 4, 2],
    ] as const) {
      const sessionDir = join(dir, 'sessions', id)
      await mkdir(sessionDir, { recursive: true })
      await renderSession(sessionDir, {
        ...result,
        id,
        actor: 'Alice',
        label: 'Login',
        purpose: 'setup',
        sequence,
        attempt,
      })
    }
    await renderScenario(dir, {
      ...scenario,
      status: 'failed',
      durationMs: 23456,
      retries: 1,
      failure: 'Assertion after the last browser closed',
    })
    await renderRun(join(f.root, 'grouped'))
    const html = await (await fetch(f.origin)).text()
    const tableStart = html.indexOf('aria-label="Scenario results"')
    const table = html.slice(tableStart, html.indexOf('</section>', tableStart))
    assert.equal((table.match(/data-testid="scenario-link"/g) ?? []).length, 1)
    assert.match(table, /23\.5s/)
    assert.match(table, /Failed/)
    assert.doesNotMatch(table, /Alice|Login|4\.2s/)
    const detail = await (
      await fetch(`${f.origin}/grouped/local-sqlite/scenario-1/index.html`)
    ).text()
    assert.match(detail, /Assertion after the last browser closed/)
    assert.equal((detail.match(/Alice · Login/g) ?? []).length, 3)
    assert.ok(detail.indexOf('sessions/z-login/') < detail.indexOf('sessions/a-login/'))
    assert.match(detail, /Attempt 2/)
    assert.match(detail, /Passed/)
    // Distinct test identities never merge, even when their display names match.
    const other = join(f.root, 'grouped', scenario.target, 'scenario-2')
    await mkdir(other)
    await renderScenario(other, {
      ...scenario,
      id: 'scenario-2',
      status: 'skipped',
      note: 'Not applicable',
    })
    const updated = await (await fetch(f.origin)).text()
    assert.equal((updated.match(/data-testid="scenario-link"/g) ?? []).length, 2)
    assert.match(
      await (await fetch(`${f.origin}/grouped/local-sqlite/scenario-2/index.html`)).text(),
      /No browser recordings/,
    )
  } finally {
    await f.close()
  }
})

test('a command displays all target scenarios and does not hide target setup failures', async () => {
  const f = await fixture()
  try {
    await f.run('combined', result.startedAt)
    const runDir = join(f.root, 'combined')
    const targetDir = join(runDir, 'authenticated-sqlite')
    await mkdir(targetDir)
    await json(join(runDir, 'run.json'), {
      startedAt: result.startedAt,
      targets: ['local-sqlite', 'authenticated-sqlite'],
    })
    await json(join(targetDir, 'target.json'), {
      name: 'authenticated-sqlite',
      state: 'starting',
      pids: [],
    })
    let html = await (await fetch(f.origin)).text()
    assert.match(html, /Waiting for scenario results from authenticated-sqlite/)
    await json(join(targetDir, 'target.json'), {
      name: 'authenticated-sqlite',
      state: 'failed',
      pids: [],
      error: 'Identity fixture failed',
    })
    html = await (await fetch(f.origin)).text()
    assert.match(html, /Target failure/)
    assert.match(html, /Identity fixture failed/)
    assert.equal((html.match(/data-testid="scenario-link"/g) ?? []).length, 1)
    for (let index = 0; index < 3; index++) {
      const id = `auth-${index}`
      const dir = join(targetDir, id)
      await mkdir(dir)
      await renderScenario(dir, {
        ...scenario,
        id,
        target: 'authenticated-sqlite',
        scenario: `Authenticated scenario ${index}`,
      })
    }
    await json(join(targetDir, 'target.json'), {
      name: 'authenticated-sqlite',
      state: 'closed',
      pids: [],
    })
    await renderRun(runDir)
    for (const url of [f.origin, `${f.origin}/combined/index.html`]) {
      html = await (await fetch(url)).text()
      assert.equal((html.match(/data-testid="scenario-link"/g) ?? []).length, 4)
      assert.match(html, /4 passed/)
      assert.match(html, /local-sqlite/)
      assert.match(html, /authenticated-sqlite/)
      assert.doesNotMatch(html, /Target failure|Waiting for scenario results/)
    }
  } finally {
    await f.close()
  }
})

test('a pending parent is not made green by completed child recordings', async () => {
  const f = await fixture()
  try {
    await f.run('pending', result.startedAt)
    await renderScenario(join(f.root, 'pending', scenario.target, scenario.id), {
      ...scenario,
      status: 'pending',
      durationMs: undefined,
    })
    const html = await (await fetch(f.origin)).text()
    assert.match(html, /Running \/ incomplete/)
    assert.match(html, /1 incomplete/)
    assert.doesNotMatch(html, /1 passed/)
  } finally {
    await f.close()
  }
})

test('empty and unfinished runs are not presented as successful tests', async () => {
  const f = await fixture()
  try {
    assert.match(await (await fetch(f.origin)).text(), /No scenario runs yet/)
    const pending = join(f.root, 'unfinished')
    await mkdir(pending)
    await json(join(pending, 'run.json'), {
      startedAt: '2026-09-08T12:00:00Z',
      targets: ['local-sqlite'],
    })
    await mkdir(join(pending, 'local-sqlite'))
    await json(join(pending, 'local-sqlite', 'target.json'), {
      name: 'local-sqlite',
      state: 'starting',
      pids: [],
    })
    let html = await (await fetch(f.origin)).text()
    assert.match(html, /No scenario results/)
    assert.doesNotMatch(html, /All tests passed/)
    await json(join(pending, 'local-sqlite', 'target.json'), {
      name: 'local-sqlite',
      state: 'failed',
      pids: [],
      error: 'Runtime could not start <unsafe>',
    })
    html = await (await fetch(f.origin)).text()
    assert.match(html, /Target failure/)
    assert.match(html, /Runtime could not start &lt;unsafe&gt;/)
    // A partial metadata write must not break the rest of the inventory.
    await writeFile(join(pending, 'run.json'), '{')
    assert.equal((await fetch(f.origin)).status, 200)
  } finally {
    await f.close()
  }
})

test('inline source and inventory never read evidence symlinked outside their directories', async () => {
  const f = await fixture()
  const outside = await mkdtemp(join(tmpdir(), 'reel-private-'))
  try {
    const dir = await f.run('safe', result.startedAt)
    const secret = join(outside, 'secret')
    await writeFile(secret, 'PRIVATE_SOURCE_CANARY')
    await rm(join(dir, 'scenario.ts'))
    await symlink(secret, join(dir, 'scenario.ts'))
    await renderSession(dir, result)
    const html = await readFile(join(dir, 'index.html'), 'utf8')
    assert.match(html, /No source snapshot available/)
    assert.doesNotMatch(html, /PRIVATE_SOURCE_CANARY/)
    await writeFile(
      secret,
      JSON.stringify({ startedAt: result.startedAt, targets: ['PRIVATE_METADATA_CANARY'] }),
    )
    await rm(join(f.root, 'safe', 'run.json'))
    await symlink(secret, join(f.root, 'safe', 'run.json'))
    const landing = await (await fetch(f.origin)).text()
    assert.match(landing, /Run metadata unavailable/)
    assert.doesNotMatch(landing, /PRIVATE_METADATA_CANARY/)
  } finally {
    await f.close()
    await rm(outside, { recursive: true, force: true })
  }
})

test('reviewers can navigate to a recording and reveal source without browser scripts', async () => {
  const f = await fixture()
  const browser = await chromium.launch()
  try {
    const dir = await f.run('latest', result.startedAt)
    await renderSession(dir, {
      ...result,
      scenario:
        'A long scenario name covering Agent credential rotation, disablement, restoration, and all retained historical attribution',
      recording: undefined,
      trace: undefined,
      steps: [],
    })
    for (const width of [1280, 390]) {
      const page = await browser.newPage({
        viewport: { width, height: 900 },
        javaScriptEnabled: false,
        colorScheme: width === 390 ? 'dark' : 'light',
      })
      const cspErrors: string[] = []
      page.on('console', (message) => {
        if (message.text().includes('Content Security Policy')) cspErrors.push(message.text())
      })
      await page.goto(f.origin)
      assert.equal(await page.title(), 'Reel')
      assert.match(
        await page.locator('body').evaluate((element) => getComputedStyle(element).fontFamily),
        /Encode Sans/,
      )
      await page.getByRole('link', { name: 'Cancel Workspace archival' }).click()
      await page.getByRole('heading', { name: 'Cancel Workspace archival' }).waitFor()
      await page.getByRole('link', { name: /Reef interaction/ }).click()
      await page.getByRole('heading', { name: 'Reef interaction' }).waitFor()
      assert.match(
        await page.getByRole('navigation', { name: 'Breadcrumb' }).innerText(),
        /All runs/,
      )
      await page.getByRole('link', { name: 'Test source', exact: true }).click()
      await page
        .getByText('Source snapshot captured for this recording.', { exact: false })
        .waitFor()
      assert.equal(
        await page.getByTestId('source').evaluate((element) => element.hasAttribute('open')),
        true,
      )
      assert.match(await page.locator('#source code').innerText(), /expect\(active\)/)
      assert.equal(
        await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth),
        true,
      )
      await page.getByRole('link', { name: 'All runs', exact: true }).click()
      await page.getByRole('heading', { name: 'Latest scenario run' }).waitFor()
      assert.deepEqual(cspErrors, [])
      await page.close()
    }
  } finally {
    await browser.close()
    await f.close()
  }
})

test('playback CSP allows only the trusted script, not inline or same-origin evidence scripts', async () => {
  const f = await fixture()
  const browser = await chromium.launch()
  try {
    const dir = await f.run('csp', result.startedAt)
    const html = await readFile(join(dir, 'index.html'), 'utf8')
    const malicious = 'document.body.dataset.compromised = "yes"'
    await writeFile(join(dir, 'evidence.js'), malicious)
    await writeFile(
      join(dir, 'index.html'),
      html.replace(
        '</body>',
        `<script>${malicious}</script><script src="evidence.js"></script></body>`,
      ),
    )
    const page = await browser.newPage()
    const response = await page.goto(
      `${f.origin}/csp/${result.target}/${result.scenarioId}/sessions/${result.id}/index.html`,
    )
    const csp = response!.headers()['content-security-policy']
    assert.ok(csp.includes(`script-src '${playbackScriptHash}'`))
    assert.doesNotMatch(csp, /unsafe-inline|unsafe-eval|script-src 'self'/)
    await page.getByRole('group', { name: 'Playback speed' }).waitFor()
    assert.equal(await page.locator('body').getAttribute('data-compromised'), null)
  } finally {
    await browser.close()
    await f.close()
  }
})
