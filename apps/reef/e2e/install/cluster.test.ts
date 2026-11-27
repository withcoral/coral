import assert from 'node:assert/strict'
import { chmod, mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { test } from 'node:test'
import { setTimeout as sleep } from 'node:timers/promises'

import { startInstall } from './cluster.ts'

// Stand-ins for the tools the setup runs. Each logs its arguments. `kind create
// cluster` hangs, as a slow cluster start does, until the run's cancellation
// kills it. `openssl` writes the files it names, so the setup reaches kind.
async function standIns(directory: string, log: string) {
  const bin = join(directory, 'bin')
  await mkdir(bin)
  const script = (body: string) =>
    `#!/bin/sh\nprintf '%s\\n' "$(basename "$0") $*" >> '${log}'\n${body}\n`
  const tools: Record<string, string> = {
    kind: 'if [ "$1" = create ]; then exec sleep 600; fi',
    openssl:
      'previous=""\nfor argument in "$@"; do\n  case "$previous" in -keyout|-out) : > "$argument" ;; esac\n  previous="$argument"\ndone',
    kubectl: '',
    helm: '',
    docker: '',
  }
  for (const [tool, body] of Object.entries(tools)) {
    await writeFile(join(bin, tool), script(body))
    await chmod(join(bin, tool), 0o755)
  }
  return bin
}

// A setup that ignores the cancellation hangs on kind until the timeout.
test(
  'a cancelled helm-install setup stops, then still saves evidence and deletes its cluster',
  { timeout: 30_000 },
  async () => {
    const directory = await mkdtemp(join(tmpdir(), 'reel-install-test-'))
    const log = join(directory, 'calls.log')
    const path = process.env.PATH
    process.env.PATH = `${await standIns(directory, log)}:${path}`
    const cancel = new AbortController()
    let setup: ReturnType<typeof startInstall> | undefined
    try {
      const evidence = join(directory, 'evidence')
      setup = startInstall({
        coralImage: 'coral:local',
        webImage: 'coral-web:local',
        keep: false,
        evidence,
        signal: cancel.signal,
      })
      const calls = () => readFile(log, 'utf8').catch(() => '')
      for (let attempt = 0; attempt < 200 && !(await calls()).includes('kind create'); attempt++)
        await sleep(50)
      assert.match(await calls(), /^kind create cluster --name (reel-[0-9a-f]{8})/m)
      cancel.abort()
      await assert.rejects(setup, { name: 'AbortError' })

      const made = (await calls()).split('\n')
      const cluster = /^kind create cluster --name (reel-[0-9a-f]{8})/m.exec(made.join('\n'))![1]
      // Setup stopped at kind: it installed nothing.
      assert.equal(
        made.some((call) => call.startsWith('helm install')),
        false,
      )
      // Cleanup ran anyway: the evidence, then the cluster's deletion.
      assert.ok(made.some((call) => call.startsWith('kubectl get events')))
      assert.ok((await readFile(join(evidence, 'events.txt'), 'utf8')) !== undefined)
      assert.ok(made.includes(`kind delete cluster --name ${cluster}`))
    } finally {
      // A failure before the cancellation must not leave the hanging kind running.
      cancel.abort()
      await setup?.catch(() => {})
      process.env.PATH = path
      await rm(directory, { recursive: true, force: true })
    }
  },
)
