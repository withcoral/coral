#!/usr/bin/env node
// Run fan-out load harness (withcoral/lagoon#452, #2391).
//
// Drives `run_fan_out_load` in `crates/coral-grpc/tests/grpc/load.rs` once per
// cell (calls × added latency), against the disposable Postgres from
// `scripts/postgres.ts` or against SQLite, and writes one JSON report per cell
// plus a Markdown summary.
//
//   node scripts/load.mjs                               # postgres, 30 calls, no added latency
//   node scripts/load.mjs --calls 30,101 --latency 0,5,20
//   node scripts/load.mjs --backend sqlite --calls 30,101
//   node scripts/load.mjs --mode hold --hold-secs 40    # today's #2391 reproduction shape
//
// Options
//   --backend postgres|sqlite   (default postgres)
//   --calls N[,N...]            parallel approval-gated calls in one Run (default 30)
//   --latency MS[,MS...]        added downstream round-trip latency through toxiproxy (default 0; postgres only)
//   --mode approve|hold         approve every request once visible, or cancel after the steady phase (default approve)
//   --hold-secs S               steady phase length (default 10)
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
// #2391 and withcoral/lagoon#408 measurements. The sampler that reads `pg_stat_activity` connects
// directly, outside the proxy, so it never competes with the pool.
import { execFile, spawn } from 'node:child_process'
import { mkdir, readFile, writeFile } from 'node:fs/promises'
import { setTimeout as delay } from 'node:timers/promises'
import { resolve } from 'node:path'
import { promisify } from 'node:util'
import { LocalPostgres } from './postgres.ts'

const exec = promisify(execFile)
const env = { ...process.env }

async function parseArgs(argv) {
  const options = {
    backend: 'postgres',
    calls: [30],
    latency: [0],
    mode: 'approve',
    holdSecs: 10,
    out: '',
    release: false,
    label: '',
  }
  const list = (value) => value.split(',').map((v) => Number(v.trim())).filter((v) => Number.isFinite(v))
  for (let i = 0; i < argv.length; i += 1) {
    const [flag, inline] = argv[i].split('=', 2)
    const next = () => (inline !== undefined ? inline : argv[++i])
    switch (flag) {
      case '--backend':
        options.backend = next()
        break
      case '--calls':
        options.calls = list(next())
        break
      case '--latency':
        options.latency = list(next())
        break
      case '--mode':
        options.mode = next()
        break
      case '--hold-secs':
        options.holdSecs = Number(next())
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
  if (options.backend === 'sqlite' && options.latency.some((ms) => ms > 0)) {
    console.error('SQLite has no network round trip; ignoring --latency')
    options.latency = [0]
  }
  return options
}

/** A toxiproxy container in front of the local Postgres, owned by this script. */
class Toxiproxy {
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

  /** Start or reuse the container and point its proxy at `upstreamPort` on the host. */
  async start(upstreamPort) {
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
      upstream: `host.docker.internal:${upstreamPort}`,
      enabled: true,
    }
    const created = await this.api('POST', '/proxies', definition)
    if (created.status === 409) {
      const updated = await this.api('POST', `/proxies/${this.proxy}`, definition)
      if (!updated.ok) throw new Error(`toxiproxy proxy update failed: ${updated.status}`)
    } else if (!created.ok) throw new Error(`toxiproxy proxy creation failed: ${created.status}`)
  }

  async setLatency(ms) {
    await this.api('DELETE', `/proxies/${this.proxy}/toxics/latency`)
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

function runCell(cellEnv, release) {
  return new Promise((resolve, reject) => {
    const args = [
      'nextest', 'run', '-p', 'coral-grpc', '--test', 'grpc',
      '--profile', 'load', '--run-ignored', 'only', '--no-capture',
      '-E', 'test(run_fan_out_load)',
    ]
    if (release) args.push('--cargo-profile', 'release')
    const child = spawn('cargo', args, { env: cellEnv, stdio: ['ignore', 'inherit', 'inherit'] })
    child.once('error', reject)
    child.once('exit', (code) => resolve(code ?? 1))
  })
}

const fmt = (v, digits = 0) => (v === null || v === undefined ? '—' : Number(v).toFixed(digits))
const dist = (d) => (d ? `${fmt(d.p50_ms)} / ${fmt(d.p95_ms)} / ${fmt(d.p99_ms)}` : '—')
const failed = (p) => (p ? `${p.failed}/${p.n}` : '—')

function summarize(reports) {
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
    'decide p95 ms',
    'max lock waits (burst)',
  ]
  const rows = reports.map((r) => {
    const phase = (name) => r.phases.find((p) => p.name === name) ?? {}
    const burst = phase('burst')
    const steady = phase('steady')
    const cell = `${r.backend} ${r.calls} calls${r.label ? ` ${r.label}` : ''}${r.debug_assertions ? ' (debug)' : ''}`
    return [
      cell,
      `${fmt(r.run.all_visible_after_ms / 1000, 1)} s${r.run.all_visible ? '' : ' (incomplete)'}`,
      `${r.run.state}${r.run.termination_kind ? `/${r.run.termination_kind}` : ''}`,
      dist(burst.begin),
      dist(steady.begin),
      `${failed(burst.ready)} / ${failed(steady.ready)}`,
      burst.sign_in ? fmt(burst.sign_in.p95_ms) : '—',
      burst.neighbor ? `${fmt(burst.neighbor.p95_ms)} / ${failed(burst.neighbor)}` : '—',
      r.invocations.admit_ms ? fmt(r.invocations.admit_ms.p95_ms) : '—',
      r.invocations.dispatch_ms ? fmt(r.invocations.dispatch_ms.p95_ms) : '—',
      r.decisions_ms ? fmt(r.decisions_ms.p95_ms) : '—',
      burst.pg ? String(burst.pg.max_lock_waits) : 'n/a',
    ]
  })
  const line = (cells) => `| ${cells.join(' | ')} |`
  return [line(header), line(header.map(() => '---')), ...rows.map(line)].join('\n')
}

const options = await parseArgs(process.argv.slice(2))
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
      const upstreamPort = Number(new URL(database.url).port || 5432)
      await toxiproxy.start(upstreamPort)
    }
  }
  for (const latency of options.latency) {
    if (toxiproxy) await toxiproxy.setLatency(latency)
    for (const calls of options.calls) {
      const label = [options.label, latency > 0 ? `+${latency}ms` : ''].filter(Boolean).join(' ')
      const report = `${out}/${options.backend}-${calls}-${latency}ms.json`
      const cellEnv = {
        ...env,
        CORAL_LOAD_CALLS: String(calls),
        CORAL_LOAD_MODE: options.mode,
        CORAL_LOAD_HOLD_SECS: String(options.holdSecs),
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
      console.error(`\n### cell: ${options.backend}, ${calls} calls, +${latency} ms, mode ${options.mode}\n`)
      const code = await runCell(cellEnv, options.release)
      if (code !== 0) throw new Error(`load cell failed with exit code ${code}`)
      reports.push(JSON.parse(await readFile(report, 'utf8')))
    }
  }
  const summary = summarize(reports)
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
