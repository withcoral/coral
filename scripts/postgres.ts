import { execFile } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import { setTimeout as delay } from 'node:timers/promises'
import { promisify } from 'node:util'

const exec = promisify(execFile)

/**
 * Own the reusable local test container and its disposable databases. Bootstrap
 * supplies the environment; no process environment or Make recipes are read here.
 */
export class LocalPostgres {
  private readonly env: NodeJS.ProcessEnv
  private readonly container: string
  private readonly image: string
  private engine: Promise<string> | undefined

  constructor(env: NodeJS.ProcessEnv) {
    this.env = { ...env }
    this.container = env.LOCAL_POSTGRES_CONTAINER || 'coral-test-postgres'
    this.image = env.LOCAL_POSTGRES_IMAGE || 'docker.io/library/postgres:17'
  }

  private selectedEngine() {
    this.engine ??= (async () => {
      if (this.env.POSTGRES_CONTAINER_ENGINE) return this.env.POSTGRES_CONTAINER_ENGINE
      for (const candidate of ['docker', 'podman']) {
        try {
          await exec(candidate, ['--version'], {
            env: this.env,
            timeout: 5000,
            killSignal: 'SIGKILL',
          })
          return candidate
        } catch {
          /* Prefer Docker, then try Podman if it is unavailable. */
        }
      }
      throw new Error('Docker or Podman is required for local Postgres')
    })()
    return this.engine
  }

  private async command(args: string[], timeout = 90_000) {
    return exec(await this.selectedEngine(), args, {
      env: this.env,
      timeout,
      killSignal: 'SIGKILL',
    })
  }

  private async exists() {
    // Listing distinguishes an absent container from a failed engine. An inspect
    // failure alone cannot tell those apart and must not silently pass cleanup.
    const { stdout } = await this.command(
      ['container', 'ls', '--all', '--format', '{{.Names}}', '--filter', `name=${this.container}`],
      10_000,
    )
    return stdout.split('\n').some((name) => name.trim() === this.container)
  }

  /** Start or reuse the container and wait for readiness before returning its URL. */
  async start() {
    const port = this.env.LOCAL_POSTGRES_PORT || ''
    if (port && (!/^\d+$/.test(port) || Number(port) > 65535))
      throw new Error('LOCAL_POSTGRES_PORT must be a port number from 0 to 65535')
    try {
      if (await this.exists()) await this.command(['start', this.container])
      else
        await this.command([
          'run',
          '-d',
          '--name',
          this.container,
          '-e',
          'POSTGRES_PASSWORD=postgres',
          '-p',
          `127.0.0.1:${port}:5432`,
          this.image,
        ])
    } catch {
      throw new Error(
        'Postgres startup failed; check Docker/Podman and the local Postgres configuration',
      )
    }
    const deadline = Date.now() + 60_000
    while (Date.now() < deadline) {
      let ready = false
      try {
        await this.command(
          ['exec', this.container, 'pg_isready', '-U', 'postgres'],
          Math.max(1, Math.min(5000, deadline - Date.now())),
        )
        ready = true
      } catch {
        /* Postgres can briefly reject connections while starting. */
      }
      if (ready) return this.url()
      await delay(Math.max(0, Math.min(1000, deadline - Date.now())))
    }
    throw new Error('Local Postgres did not become ready within 60 seconds')
  }

  /** Inspect the current loopback mapping without creating or starting anything. */
  async url() {
    try {
      const { stdout } = await this.command(['port', this.container, '5432/tcp'], 10_000)
      const match = /^127\.0\.0\.1:(\d+)$/.exec(stdout.trim())
      if (!match || Number(match[1]) < 1 || Number(match[1]) > 65535) throw new Error()
      return `postgres://postgres:postgres@127.0.0.1:${Number(match[1])}/postgres`
    } catch {
      throw new Error('Local Postgres has no usable loopback port; run make postgres-start')
    }
  }

  async stop() {
    try {
      if (await this.exists()) await this.command(['stop', this.container], 30_000)
    } catch {
      throw new Error('Postgres stop failed; check the container engine')
    }
  }

  async clean() {
    try {
      if (await this.exists()) await this.command(['rm', '-f', this.container], 30_000)
    } catch {
      throw new Error('Postgres container removal failed; check the container engine')
    }
  }

  /** Close after consumer processes stop. An explicit URL remains caller-owned. */
  async provision(url?: string) {
    if (url) {
      try {
        const parsed = new URL(url)
        if (
          !['postgres:', 'postgresql:'].includes(parsed.protocol) ||
          !parsed.hostname ||
          parsed.pathname.length < 2 ||
          parsed.hash
        )
          throw new Error()
      } catch {
        throw new Error('Supply a disposable Postgres database URL')
      }
      return { url, close: async () => {} }
    }
    const address = new URL(await this.start())
    const database = `coral_test_${randomUUID().replaceAll('-', '_')}`
    address.pathname = `/${database}`
    try {
      await this.command(['exec', this.container, 'createdb', '-U', 'postgres', database])
    } catch {
      // A timed-out client may still have committed CREATE DATABASE.
      try {
        await this.command([
          'exec',
          this.container,
          'dropdb',
          '-U',
          'postgres',
          '--if-exists',
          database,
        ])
      } catch {
        throw new Error(`Postgres setup and cleanup failed; remove disposable database ${database}`)
      }
      throw new Error('Postgres database creation failed; the disposable database was removed')
    }
    let closing: Promise<void> | undefined
    const close = () =>
      (closing ??= this.command(['exec', this.container, 'dropdb', '-U', 'postgres', database])
        .then(() => {})
        .catch(() => {
          throw new Error(`Postgres cleanup failed; remove disposable database ${database}`)
        }))
    return { url: address.href, close }
  }
}
