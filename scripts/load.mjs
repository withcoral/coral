#!/usr/bin/env node
// Run coordination load harness (#2379, #2391).
//
// Drives the ignored cells in `crates/coral-grpc/tests/grpc/load.rs` against the
// disposable Postgres from `scripts/postgres.ts` or against SQLite, once per added
// latency, and writes one JSON report per cell plus `summary.md` / `summary.json`.
//
//   node scripts/load.mjs                                # postgres, every cell, no added latency
//   node scripts/load.mjs --calls 30,101 --latency 0,5,20 --release
//   node scripts/load.mjs --backend sqlite --calls 30,101
//   node scripts/load.mjs --cells fanout --mode hold --hold-secs 40   # today's #2391 reproduction shape
//
// Cells (per added latency, in this order)
//   fanout       one Run makes N parallel approval-gated calls, once per --calls value
//   emit         one Program emits --emits small values, then --large-emits values of 64 KiB
//   concurrent   --runs one-call, no-approval Runs start at once against the default pool
//
// Options
//   --backend postgres|sqlite   (default postgres)
//   --cells NAME[,NAME...]      cells to run (default fanout,emit,concurrent)
//   --calls N[,N...]            fanout: parallel approval-gated calls in one Run (default 30)
//   --mode approve|hold         fanout: approve every request once visible, or cancel after the steady phase (default approve)
//   --hold-secs S               fanout: steady phase length (default 10)
//   --emits N                   emit: small values in one Run (default 1000)
//   --large-emits N             emit: 64 KiB values in one Run (default 64)
//   --emit-interval MS          emit: pace the Program at one emit per MS (default 0: as fast as emit answers)
//   --runs N                    concurrent: Runs started at once (default 20)
//   --latency MS[,MS...]        added downstream round-trip latency through toxiproxy (default 0; postgres only)
//   --out DIR                   report directory (default target/load/<timestamp>)
//   --release                   build the test target with the release profile
//   --label TEXT                free text copied into every report
//
// Environment: the same Postgres knobs as `make postgres-tests`
// (CORAL_TEST_POSTGRES_URL, LOCAL_POSTGRES_*, POSTGRES_CONTAINER_ENGINE), plus
// LOAD_TOXIPROXY_CONTAINER (default coral-load-toxiproxy), LOAD_TOXIPROXY_IMAGE
// (default ghcr.io/shopify/toxiproxy:2.12.0), LOAD_TOXIPROXY_API_PORT (default
// 28474) and LOAD_TOXIPROXY_PG_PORT (default 28432).
//
// Latency is added on the downstream stream only (server to client), like the
// #2391 measurements. Measurement reads and the `pg_stat_activity` sampler connect
// directly, outside the proxy, so they never compete with the pool and the added
// latency never blunts what they time.
import { execFile, spawn } from 'node:child_process'
import { mkdir, readFile, writeFile } from 'node:fs/promises'
import { setTimeout as delay } from 'node:timers/promises'
import { resolve } from 'node:path'
import { pathToFileURL } from 'node:url'
import { promisify } from 'node:util'
import { LocalPostgres } from './postgres.ts'

const exec = promisify(execFile)
const env = { ...process.env }
const CELLS = ['fanout', 'emit', 'concurrent']
const LARGE_EMIT_BYTES = 64 * 1024

export async function parseArgs(argv) {
  const options = {
    backend: 'postgres',
    cells: CELLS,
    calls: [30],
    latency: [0],
    mode: 'approve',
    holdSecs: 10,
    emits: 1000,
    emitInterval: 0,
    largeEmits: 64,
    runs: 20,
    out: '',
    release: false,
    label: '',
  }
  for (let i = 0; i < argv.length; i += 1) {
    const [flag, inline] = argv[i].split('=', 2)
    const next = () => {
      const value = inline !== undefined ? inline : argv[++i]
      if (value === undefined || value === '') throw new Error(`${flag} needs a value`)
      return value
    }
    /** A non-negative integer, or at least `min`. */
    const integer = (value, min = 0) => {
      const n = value.trim() === '' ? Number.NaN : Number(value)
      if (!Number.isInteger(n) || n < min) throw new Error(`${flag} needs an integer of at least ${min}, got ${JSON.stringify(value)}`)
      return n
    }
    const list = (value, min) => {
      const values = value.split(',').map((v) => integer(v.trim(), min))
      if (values.length === 0) throw new Error(`${flag} needs at least one value`)
      return values
    }
    switch (flag) {
      case '--backend':
        options.backend = next()
        break
      case '--cells':
        options.cells = next().split(',').map((v) => v.trim()).filter(Boolean)
        if (options.cells.length === 0) throw new Error('--cells needs at least one cell')
        break
      case '--calls':
        options.calls = list(next(), 1)
        break
      case '--latency':
        options.latency = list(next(), 0)
        break
      case '--mode':
        options.mode = next()
        break
      case '--hold-secs':
        options.holdSecs = integer(next(), 0)
        break
      case '--emits':
        options.emits = integer(next(), 1)
        break
      case '--large-emits':
        options.largeEmits = integer(next(), 1)
        break
      case '--emit-interval':
        options.emitInterval = integer(next(), 0)
        break
      case '--runs':
        options.runs = integer(next(), 1)
        break
      case '--out':
        options.out = next()
        break
      case '--release':
        options.release = true
        break
      case '--label':
        options.label = next()
        break
      case '--help':
      case '-h':
        console.log((await readFile(new URL(import.meta.url))).toString().split('\n').filter((l) => l.startsWith('//')).map((l) => l.slice(3)).join('\n'))
        process.exit(0)
        break
      default:
        throw new Error(`Unknown option ${flag}`)
    }
  }
  if (!['postgres', 'sqlite'].includes(options.backend)) throw new Error('--backend is postgres or sqlite')
  if (!['approve', 'hold'].includes(options.mode)) throw new Error('--mode is approve or hold')
  const unknown = options.cells.filter((cell) => !CELLS.includes(cell))
  if (unknown.length > 0) throw new Error(`Unknown cell ${unknown.join(', ')}; cells are ${CELLS.join(', ')}`)
  if (options.backend === 'sqlite' && options.latency.some((ms) => ms > 0)) {
    console.error('SQLite has no network round trip; ignoring --latency')
    options.latency = [0]
  }
  return options
}

/** A toxiproxy container in front of the local Postgres, owned by this script. */
export class Toxiproxy {
  constructor(env) {
    this.env = env
    this.container = env.LOAD_TOXIPROXY_CONTAINER || 'coral-load-toxiproxy'
    this.image = env.LOAD_TOXIPROXY_IMAGE || 'ghcr.io/shopify/toxiproxy:2.12.0'
    this.apiPort = Number(env.LOAD_TOXIPROXY_API_PORT || 28474)
    this.pgPort = Number(env.LOAD_TOXIPROXY_PG_PORT || 28432)
    this.proxy = 'coral-load-pg'
    this.engine = undefined
  }

  async selectedEngine() {
    if (this.engine) return this.engine
    if (this.env.POSTGRES_CONTAINER_ENGINE) return (this.engine = this.env.POSTGRES_CONTAINER_ENGINE)
    for (const candidate of ['docker', 'podman']) {
      try {
        await exec(candidate, ['--version'], { env: this.env, timeout: 5000 })
        return (this.engine = candidate)
      } catch {
        /* try the next engine */
      }
    }
    throw new Error('Docker or Podman is required for added latency')
  }

  async command(args, timeout = 90_000) {
    return exec(await this.selectedEngine(), args, { env: this.env, timeout })
  }

  async api(method, path, body) {
    const response = await fetch(`http://127.0.0.1:${this.apiPort}${path}`, {
      method,
      headers: body ? { 'content-type': 'application/json' } : {},
      body: body ? JSON.stringify(body) : undefined,
    })
    return response
  }

  /** Start or reuse the container and point its proxy at the Postgres behind `databaseUrl`. */
  async start(databaseUrl) {
    const upstream = new URL(databaseUrl)
    const upstreamPort = Number(upstream.port || 5432)
    // The container reaches a Postgres on this machine through Docker's host alias;
    // any other host is forwarded as configured.
    const loopback = ['localhost', '127.0.0.1', '[::1]', '::1']
    const upstreamHost = loopback.includes(upstream.hostname) ? 'host.docker.internal' : upstream.hostname
    const { stdout } = await this.command([
      'container', 'ls', '--all', '--format', '{{.Names}}', '--filter', `name=${this.container}`,
    ], 10_000)
    const exists = stdout.split('\n').some((name) => name.trim() === this.container)
    if (exists) await this.command(['start', this.container])
    else
      await this.command([
        'run', '-d', '--name', this.container,
        '--add-host', 'host.docker.internal:host-gateway',
        '-p', `127.0.0.1:${this.apiPort}:8474`,
        '-p', `127.0.0.1:${this.pgPort}:${this.pgPort}`,
        this.image,
      ])
    for (let attempt = 0; attempt < 60; attempt += 1) {
      try {
        if ((await this.api('GET', '/version')).ok) break
      } catch {
        /* not listening yet */
      }
      if (attempt === 59) throw new Error('toxiproxy did not become ready within 30 seconds')
      await delay(500)
    }
    const definition = {
      name: this.proxy,
      listen: `0.0.0.0:${this.pgPort}`,
      upstream: `${upstreamHost}:${upstreamPort}`,
      enabled: true,
    }
    const created = await this.api('POST', '/proxies', definition)
    if (created.status === 409) {
      const updated = await this.api('POST', `/proxies/${this.proxy}`, definition)
      if (!updated.ok) throw new Error(`toxiproxy proxy update failed: ${updated.status}`)
    } else if (!created.ok) throw new Error(`toxiproxy proxy creation failed: ${created.status}`)
  }

  async setLatency(ms) {
    const removed = await this.api('DELETE', `/proxies/${this.proxy}/toxics/latency`)
    if (!removed.ok && removed.status !== 404) throw new Error(`toxiproxy latency toxic removal failed: ${removed.status}`)
    if (ms <= 0) return
    const response = await this.api('POST', `/proxies/${this.proxy}/toxics`, {
      name: 'latency',
      type: 'latency',
      stream: 'downstream',
      toxicity: 1,
      attributes: { latency: ms, jitter: 0 },
    })
    if (!response.ok) throw new Error(`toxiproxy latency toxic failed: ${response.status}`)
  }

  url(databaseUrl) {
    const address = new URL(databaseUrl)
    address.hostname = '127.0.0.1'
    address.port = String(this.pgPort)
    return address.href
  }
}

/** The cell running right now, so an interrupt can stop it before cleanup. */
let currentCell = null
let interrupted = null

function runTest(test, cellEnv, release) {
  return new Promise((resolve, reject) => {
    const args = [
      'nextest', 'run', '-p', 'coral-grpc', '--test', 'grpc',
      '--profile', 'load', '--run-ignored', 'only', '--no-capture',
      '-E', `test(${test})`,
    ]
    if (release) args.push('--cargo-profile', 'release')
    const child = spawn('cargo', args, { env: cellEnv, stdio: ['ignore', 'inherit', 'inherit'] })
    currentCell = child
    child.once('error', reject)
    child.once('exit', (code, signal) => {
      currentCell = null
      resolve(signal ? 1 : (code ?? 1))
    })
  })
}


/** The cells one latency step runs: test name, report file stem, knobs, and a line for the log. */
export function plan(options, latency) {
  const cells = []
  for (const cell of options.cells) {
    if (cell === 'fanout') {
      for (const calls of options.calls) {
        cells.push({
          test: 'run_fan_out_load',
          stem: `fanout-${calls}`,
          knobs: { CORAL_LOAD_CALLS: String(calls), CORAL_LOAD_MODE: options.mode, CORAL_LOAD_HOLD_SECS: String(options.holdSecs) },
          title: `fan-out, ${calls} calls, mode ${options.mode}`,
        })
      }
    } else if (cell === 'emit') {
      const paced = options.emitInterval > 0 ? `-paced${options.emitInterval}` : ''
      const pacing = options.emitInterval > 0 ? `, one per ${options.emitInterval} ms` : ''
      const interval = { CORAL_LOAD_EMIT_INTERVAL_MS: String(options.emitInterval) }
      cells.push({
        test: 'run_emit_load',
        stem: `emit-small${paced}`,
        knobs: { CORAL_LOAD_EMITS: String(options.emits), CORAL_LOAD_EMIT_BYTES: '0', ...interval },
        title: `emit, ${options.emits} small values${pacing}`,
      })
      cells.push({
        test: 'run_emit_load',
        stem: `emit-64k${paced}`,
        knobs: { CORAL_LOAD_EMITS: String(options.largeEmits), CORAL_LOAD_EMIT_BYTES: String(LARGE_EMIT_BYTES), ...interval },
        title: `emit, ${options.largeEmits} values of 64 KiB${pacing}`,
      })
    } else if (cell === 'concurrent') {
      cells.push({
        test: 'run_concurrent_runs_load',
        stem: `concurrent-${options.runs}`,
        knobs: { CORAL_LOAD_RUNS: String(options.runs) },
        title: `concurrent, ${options.runs} Runs`,
      })
    }
  }
  return cells.map((cell) => ({ ...cell, stem: `${options.backend}-${cell.stem}-${latency}ms` }))
}

const fmt = (v, digits = 0) => (v === null || v === undefined ? '—' : Number(v).toFixed(digits))
const dist3 = (d) => (d && d.n > 0 ? `${fmt(d.p50_ms)} / ${fmt(d.p95_ms)} / ${fmt(d.p99_ms)}` : '—')
const dist2 = (d) => (d && d.n > 0 ? `${fmt(d.p50_ms)} / ${fmt(d.p95_ms)}` : '—')
const failed = (p) => (p ? `${p.failed}/${p.n}` : '—')
const p95 = (p) => (p && p.n > 0 ? fmt(p.p95_ms) : '—')
const p95Failed = (p) => (p && p.n > 0 ? `${fmt(p.p95_ms)} / ${failed(p)}` : '—')
const seconds = (ms) => `${fmt(ms / 1000, 1)} s`
const phaseOf = (r, name) => r.phases.find((p) => p.name === name) ?? {}
const cellName = (r, detail) =>
  `${r.backend} ${detail}${r.label ? ` ${r.label}` : ''}${r.debug_assertions ? ' (debug)' : ''}` +
  `${r.sampler_error ? ' (sampler failed)' : ''}${r.probes_aborted_in_flight ? ` (${r.probes_aborted_in_flight} probes cut off)` : ''}`
const byState = (counts) => Object.entries(counts ?? {}).map(([state, n]) => `${state} ${n}`).join(', ') || '—'
const table = (header, rows) => {
  const line = (cells) => `| ${cells.join(' | ')} |`
  return [line(header), line(header.map(() => '---')), ...rows.map(line)].join('\n')
}

function summarizeFanOut(reports) {
  const header = [
    'cell',
    'all visible after',
    'Run',
    'begin p50/p95/p99 ms (burst)',
    'begin p50/p95/p99 ms (steady)',
    'ready failed (burst / steady)',
    'sign-in p95 ms (burst)',
    'neighbor p95 ms / failed (burst)',
    'admit p95 ms',
    'dispatch p95 ms',
    'max dispatching',
    'decide p95 ms',
    'max lock waits (burst)',
  ]
  const rows = reports.map((r) => {
    const burst = phaseOf(r, 'burst')
    const steady = phaseOf(r, 'steady')
    return [
      cellName(r, `${r.calls} calls`),
      `${seconds(r.run.all_visible_after_ms)}${r.run.all_visible ? '' : ' (incomplete)'}`,
      `${r.run.state}${r.run.termination_kind ? `/${r.run.termination_kind}` : ''}`,
      dist3(burst.begin),
      dist3(steady.begin),
      `${failed(burst.ready)} / ${failed(steady.ready)}`,
      p95(burst.sign_in),
      p95Failed(burst.neighbor),
      p95(r.invocations.admit_ms),
      p95(r.invocations.dispatch_ms),
      r.invocations.max_dispatching_at_once === undefined ? 'n/a' : String(r.invocations.max_dispatching_at_once),
      p95(r.decisions_ms),
      burst.pg ? String(burst.pg.max_lock_waits) : 'n/a',
    ]
  })
  return table(header, rows)
}

function summarizeEmit(reports) {
  const header = [
    'cell',
    'value bytes',
    'Run',
    'emitting',
    'emits/s',
    'emit ack p50/p95 ms',
    'held p50/p95 ms',
    'recorded p50/p95 ms',
    'visible p50/p95 ms',
    'begin p50/p95/p99 ms (emitting)',
    'ready failed (emitting)',
    'sign-in p95 ms (emitting)',
    'neighbor p95 ms / failed (emitting)',
    'max lock waits (emitting)',
  ]
  const rows = reports.map((r) => {
    const emitting = phaseOf(r, 'emitting')
    const e = r.emissions
    return [
      cellName(r, `${r.emits} emits${r.interval_ms > 0 ? ` / ${r.interval_ms} ms` : ''}`),
      String(r.value_bytes),
      `${r.run.state}${e.observed === r.emits ? '' : ` (${e.observed}/${r.emits} observed)`}`,
      seconds(r.run.emitting_ms),
      fmt(e.per_s),
      dist2(e.emit_ack_ms),
      dist2(e.held_ms),
      dist2(e.recorded_ms),
      dist2(e.visible_ms),
      dist3(emitting.begin),
      failed(emitting.ready),
      p95(emitting.sign_in),
      p95Failed(emitting.neighbor),
      emitting.pg ? String(emitting.pg.max_lock_waits) : 'n/a',
    ]
  })
  return table(header, rows)
}

function summarizeConcurrent(reports) {
  const header = [
    'cell',
    'all ended after',
    'Runs by state',
    'start p50/p95 ms',
    'total p50/p95 ms',
    'admit p95 ms',
    'dispatch p95 ms',
    'begin p50/p95/p99 ms (burst)',
    'ready failed (burst)',
    'sign-in p50/p95 ms (burst)',
    'neighbor p95 ms / failed (burst)',
    'max connections (burst)',
    'max lock waits (burst)',
  ]
  const rows = reports.map((r) => {
    const burst = phaseOf(r, 'burst')
    return [
      cellName(r, `${r.runs.concurrency} Runs`),
      seconds(r.runs.all_ended_after_ms),
      byState(r.runs.by_state),
      dist2(r.runs.start_ms),
      dist2(r.runs.total_ms),
      p95(r.invocations.admit_ms),
      p95(r.invocations.dispatch_ms),
      dist3(burst.begin),
      failed(burst.ready),
      dist2(burst.sign_in),
      p95Failed(burst.neighbor),
      burst.pg ? String(burst.pg.max_connections) : 'n/a',
      burst.pg ? String(burst.pg.max_lock_waits) : 'n/a',
    ]
  })
  return table(header, rows)
}

export function summarize(reports, options) {
  const sections = [
    ['Fan-out', 'fanout', summarizeFanOut],
    ['Emit', 'emit', summarizeEmit],
    ['Concurrent Runs', 'concurrent', summarizeConcurrent],
  ]
  const parts = [
    `# Load harness: ${options.backend}${options.release ? ', release' : ', debug'}`,
    '',
    `Latency steps: ${options.latency.map((ms) => `+${ms} ms`).join(', ')}. Probe latencies in milliseconds; "failed" counts samples outside their budget or in error.`,
  ]
  for (const [title, cell, render] of sections) {
    const rows = reports.filter((r) => r.cell === cell)
    if (rows.length === 0) continue
    parts.push('', `## ${title}`, '', render(rows))
  }
  return parts.join('\n')
}

/** Runs the matrix for the given argv; the module's exports stay importable by tests. */
export async function main(argv) {
  let options
  try {
    options = await parseArgs(argv)
  } catch (error) {
    console.error(`${error.message}\nSee node scripts/load.mjs --help`)
    process.exit(2)
  }
  // An interrupted run still closes the database and clears the proxy latency:
  // stop the cell, remember the signal, and let the flow below reach `finally`.
  for (const signal of ['SIGINT', 'SIGTERM']) {
    process.once(signal, () => {
      interrupted = signal
      console.error(`\n${signal}: stopping the current cell and cleaning up`)
      currentCell?.kill('SIGTERM')
    })
  }
  const stamp = new Date().toISOString().replaceAll(/[:.]/g, '-')
  const out = resolve(options.out || `target/load/${stamp}`)
  await mkdir(out, { recursive: true })

  let database
  let toxiproxy
  const reports = []
  try {
    if (options.backend === 'postgres') {
      const postgres = new LocalPostgres(env)
      database = await postgres.provision(env.CORAL_TEST_POSTGRES_URL)
      if (options.latency.some((ms) => ms > 0)) {
        toxiproxy = new Toxiproxy(env)
        await toxiproxy.start(database.url)
      }
    }
    for (const latency of options.latency) {
      if (toxiproxy) await toxiproxy.setLatency(latency)
      const label = [options.label, latency > 0 ? `+${latency}ms` : ''].filter(Boolean).join(' ')
      for (const cell of plan(options, latency)) {
        const report = `${out}/${cell.stem}.json`
        const cellEnv = {
          ...env,
          ...cell.knobs,
          CORAL_LOAD_REPORT: report,
          CORAL_LOAD_LABEL: label,
        }
        if (options.backend === 'postgres') {
          cellEnv.CORAL_TEST_DATABASE = 'postgres'
          cellEnv.CORAL_TEST_POSTGRES_URL = latency > 0 ? toxiproxy.url(database.url) : database.url
          cellEnv.CORAL_LOAD_SAMPLER_URL = database.url
        } else {
          delete cellEnv.CORAL_TEST_DATABASE
          delete cellEnv.CORAL_TEST_POSTGRES_URL
          delete cellEnv.CORAL_LOAD_SAMPLER_URL
        }
        console.error(`\n### cell: ${options.backend}, ${cell.title}, +${latency} ms\n`)
        const code = await runTest(cell.test, cellEnv, options.release)
        if (interrupted) throw new Error(`interrupted by ${interrupted} during cell ${cell.stem}`)
        if (code !== 0) throw new Error(`load cell ${cell.stem} failed with exit code ${code}`)
        reports.push(JSON.parse(await readFile(report, 'utf8')))
      }
    }
    const summary = summarize(reports, options)
    await writeFile(`${out}/summary.md`, `${summary}\n`)
    await writeFile(`${out}/summary.json`, JSON.stringify(reports, null, 2))
    console.log(`\n${summary}\n\nreports: ${out}`)
  } catch (error) {
    console.error(error.message)
    process.exitCode = 1
  } finally {
    if (toxiproxy) {
      try {
        await toxiproxy.setLatency(0)
      } catch {
        /* the container may already be gone */
      }
    }
    try {
      await database?.close()
    } catch (error) {
      console.error(error.message)
      process.exitCode = 1
    }
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) await main(process.argv.slice(2))
