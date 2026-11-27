import assert from 'node:assert/strict'
import { chmod, mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { test } from 'node:test'
import { setTimeout as sleep } from 'node:timers/promises'

import { startInstall } from './cluster.ts'

// Stand-ins for the tools the setup runs. Each logs its arguments. `kind create
// cluster` hangs, as a slow cluster start does, until the run's cancellation
// kills it, unless `hang` is false. `openssl` writes the files it names, so the
// setup reaches kind. `helm` keeps each values file it is given in values.log.
// `docker exec … curl` exits with `egress`: 0 when the node reaches the
// internet.
async function standIns(directory: string, log: string, { hang = true, egress = 7 } = {}) {
  const bin = join(directory, 'bin')
  await mkdir(bin)
  const script = (body: string) =>
    `#!/bin/sh\nprintf '%s\\n' "$(basename "$0") $*" >> '${log}'\n${body}\n`
  const tools: Record<string, string> = {
    kind: hang ? 'if [ "$1" = create ]; then exec sleep 600; fi' : '',
    openssl:
      'previous=""\nfor argument in "$@"; do\n  case "$previous" in -keyout|-out) : > "$argument" ;; esac\n  previous="$argument"\ndone',
    kubectl: [
      'case "$*" in',
      `  exec*) exit ${egress} ;;`,
      "  *jsonpath=\\{.status.addresses*) printf '172.18.0.9' ;;",
      "  *jsonpath=\\{.data.Corefile*) printf '.:53 {\\n    forward . /etc/resolv.conf\\n}\\n' ;;",
      'esac',
    ].join('\n'),
    helm: `previous=""\nfor argument in "$@"; do\n  if [ "$previous" = --values ]; then { cat "$argument"; echo; } >> '${join(directory, 'values.log')}'; fi\n  previous="$argument"\ndone`,
    docker: [
      'case "$*" in',
      `  exec*curl*) exit ${egress} ;;`,
      '  port*) echo 127.0.0.1:5999 ;;',
      "  network\\ inspect*) echo '172.18.0.0/16 fc00:f853:ccd:e793::/64' ;;",
      'esac',
    ].join('\n'),
    crane: '',
  }
  for (const [tool, body] of Object.entries(tools)) {
    await writeFile(join(bin, tool), script(body))
    await chmod(join(bin, tool), 0o755)
  }
  return bin
}

const CORAL = `ghcr.io/withcoral/coral:1.2.3@sha256:${'a'.repeat(64)}`
const ENVOY = `docker.io/envoyproxy/envoy@sha256:${'b'.repeat(64)}`

/** Runs an air-gapped setup on stand-ins; returns its calls and the values Helm saw. */
async function airGapped(egress: number) {
  const directory = await mkdtemp(join(tmpdir(), 'reel-install-test-'))
  const log = join(directory, 'calls.log')
  const path = process.env.PATH
  process.env.PATH = `${await standIns(directory, log, { hang: false, egress })}:${path}`
  try {
    const imagesTxt = join(directory, 'images.txt')
    await writeFile(imagesTxt, `${CORAL}\n${ENVOY}\n`)
    const chart = join(directory, 'coral-1.2.3.tgz')
    let error: unknown
    try {
      const install = await startInstall({
        kubernetes: 'oldest',
        chart,
        images: { mirror: imagesTxt },
        keep: false,
        evidence: join(directory, 'evidence'),
        signal: new AbortController().signal,
      })
      await install.stop()
    } catch (caught) {
      error = caught
    }
    return {
      error,
      chart,
      calls: (await readFile(log, 'utf8')).split('\n').filter(Boolean),
      values: await readFile(join(directory, 'values.log'), 'utf8').catch(() => ''),
    }
  } finally {
    process.env.PATH = path
    await rm(directory, { recursive: true, force: true })
  }
}

test("an air-gapped setup mirrors images.txt with the docs' loop, blocks egress, then installs", async () => {
  const { error, chart, calls, values } = await airGapped(7)
  assert.equal(error, undefined)
  const at = (pattern: RegExp) => {
    const index = calls.findIndex((call) => pattern.test(call))
    assert.notEqual(index, -1, `no call matches ${pattern}`)
    return index
  }
  // The oldest Kubernetes the chart supports.
  at(/^kind create cluster .*--image kindest\/node:v1\.35\./)
  // The docs' loop keeps Coral's tag and Envoy's digest.
  const copies = [
    at(/^crane copy \S+coral:1\.2\.3@sha256:a{64} 127\.0\.0\.1:5999\/withcoral\/coral:1\.2\.3$/),
    at(/^crane copy \S+envoy@sha256:b{64} 127\.0\.0\.1:5999\/envoyproxy\/envoy@sha256:b{64}$/),
  ]
  const block = at(/^docker exec .*iptables/)
  const check = at(/^docker exec .*curl/)
  const install = at(/^helm install coral /)
  assert.ok(Math.max(...copies, block) < check && check < install)
  assert.ok(calls[install].includes(` ${chart} `))
  assert.match(calls[install], /--values \S+\/charts\/coral\/examples\/values-airgapped\.yaml /)
  // Every image comes from the mirror: the cluster is given none.
  assert.equal(
    calls.some((call) => call.startsWith('kind load')),
    false,
  )
  const registry = /^docker run .*--name (reel-[0-9a-f]{8}-registry) /m.exec(calls.join('\n'))![1]
  assert.match(values, new RegExp(`"imageRegistry":"${registry}:5000"`))
  assert.doesNotMatch(values, /pullPolicy/)
  // Cleanup removes the registry with the cluster.
  at(new RegExp(`^docker rm -f ${registry}$`))
})

test('an air-gapped setup stops when its node still reaches the internet', async () => {
  const { error, calls } = await airGapped(0)
  assert.match(
    String(error),
    /Blocking egress failed: the node and a pod still reached the internet/,
  )
  assert.equal(
    calls.some((call) => call.startsWith('helm install coral ')),
    false,
  )
})

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
        kubernetes: 'newest',
        chart: 'charts/coral',
        images: { load: { coral: 'coral:local', web: 'coral-web:local' } },
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
