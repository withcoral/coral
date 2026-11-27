import assert from 'node:assert/strict'
import { mkdtemp, mkdir, readFile, rm, symlink, writeFile } from 'node:fs/promises'
import { get } from 'node:http'
import { tmpdir } from 'node:os'
import { basename, join } from 'node:path'
import { test } from 'node:test'

import { snapshotSource } from './artifacts.ts'
import {
  bootstrapConfig,
  commandOptions,
  desktopExecutable,
  loopbackFixtures,
  reefRoot,
} from './config.ts'
import { createServer } from 'node:http'
import { listen, validateFixtureTLS } from './provider-fixtures.ts'
import { detectFfmpeg } from './ffmpeg.ts'
import { resultsServer } from './results-server.ts'
import { startIdentityProvider } from './oidc-fixture.ts'
import { reservedPortTaken, startStack } from './stack.ts'

test('Reel defaults to main and reserves database selection for that suite', () => {
  assert.deepEqual(commandOptions([]), { suite: 'main', database: 'sqlite', vitest: [] })
  assert.deepEqual(commandOptions(['--suite', 'main']), commandOptions([]))
  assert.deepEqual(commandOptions(['--database', 'postgres', 'accounts/', '-t', 'OAuth']), {
    suite: 'main',
    database: 'postgres',
    vitest: ['accounts/', '-t', 'OAuth'],
  })
  assert.deepEqual(commandOptions(['--suite=local-mode']), {
    suite: 'local-mode',
    database: 'sqlite',
    vitest: [],
  })
  assert.deepEqual(commandOptions(['--suite', 'desktop']), {
    suite: 'desktop',
    database: 'sqlite',
    vitest: [],
  })
  // Several Coral Instances share one database only on Postgres, so the suite selects it.
  assert.deepEqual(commandOptions(['--suite', 'multi-instance']), {
    suite: 'multi-instance',
    database: 'postgres',
    vitest: [],
  })
  for (const args of [
    ['--suite'],
    ['--database'],
    ['--suite', 'missing'],
    ['--suite', 'product'],
    ['--suite', 'local'],
    ['--database', 'mysql'],
    ['--suite', 'local-mode', '--database', 'postgres'],
    ['--suite', 'desktop', '--database', 'sqlite'],
    ['--suite', 'multi-instance', '--database', 'postgres'],
    ['--desktop'],
    ['--project', 'local-sqlite'],
    ['--suite', 'local-mode', '--suite', 'main'],
  ])
    assert.throws(() => commandOptions(args))
})

test('an explicit run identity keeps container evidence in its final directory without escaping runs', () => {
  const first = bootstrapConfig({}).runDir
  const id = basename(first)
  assert.equal(bootstrapConfig({ E2E_RUN_ID: id }).runDir, first)
  assert.notEqual(bootstrapConfig({}).runDir, first, 'native commands retain fresh identities')
  for (const value of ['', '..', '../outside', '/tmp/outside', `${id}/nested`, `.${id}`])
    assert.throws(() => bootstrapConfig({ E2E_RUN_ID: value }), /run identifier/)
})

test('routine setup needs no host provisioning and Postgres remains runner-owned', () => {
  assert.equal(bootstrapConfig({}).fixtures, undefined)
  assert.throws(() => bootstrapConfig({ E2E_DATABASE: 'postgres' }), /--database postgres/)
  assert.equal(
    bootstrapConfig({
      E2E_DATABASE: 'postgres',
      E2E_POSTGRES_TEST_URL: 'postgres://fixture@localhost/disposable',
    }).database,
    'postgres',
  )
  assert.throws(
    () => bootstrapConfig({ E2E_SUITE: 'multi-instance' }),
    /multi-instance suite runs on Postgres/,
  )
  assert.equal(
    bootstrapConfig({
      E2E_SUITE: 'multi-instance',
      E2E_DATABASE: 'postgres',
      E2E_POSTGRES_TEST_URL: 'postgres://fixture@localhost/disposable',
    }).postgresURL,
    'postgres://fixture@localhost/disposable',
  )
})

test('the helm-install suite drives a cluster that brings its own Postgres and sign-in settings', () => {
  assert.deepEqual(commandOptions(['--suite', 'helm-install']), {
    suite: 'helm-install',
    database: 'postgres',
    vitest: [],
  })
  assert.throws(
    () => commandOptions(['--suite', 'helm-install', '--database', 'postgres']),
    /Only the main suite selects a database/,
  )
  // Without the host's setup, the suite fails at once, not at sign-in.
  assert.throws(
    () => bootstrapConfig({ E2E_SUITE: 'helm-install', E2E_DATABASE: 'postgres' }),
    /install\/cluster\.ts sets up/,
  )
  const ownerSubject = 'reel-00000000-0000-4000-8000-000000000000'
  const config = bootstrapConfig({
    E2E_SUITE: 'helm-install',
    E2E_DATABASE: 'postgres',
    E2E_INSTALL_OWNER_SUBJECT: ownerSubject,
    E2E_INSTALL_CLIENT_SECRET: 'fixture-secret',
  })
  // Reel provisions no database for it, so none is required or set.
  assert.equal(config.postgresURL, undefined)
  assert.equal(config.install?.ownerSubject, ownerSubject)
  assert.equal(config.install?.caFile, '/run/reel-install/ca.pem')
})

const routableFixtures = {
  // Configuration-only example; this test never contacts the address.
  E2E_FIXTURE_BIND_HOST: '0.0.0.0',
  E2E_FIXTURE_PUBLIC_HOST: '11.0.0.1',
  E2E_FIXTURE_TLS_CERT: '/fixture/cert.pem',
  E2E_FIXTURE_TLS_KEY: '/fixture/key.pem',
  E2E_FIXTURE_CA: '/fixture/ca.pem',
}

test('fixture bootstrap preserves explicitly configured RFC1918 HTTPS addresses', () => {
  for (const host of ['10.254.254.254', '172.16.0.1', '192.168.1.1']) {
    const config = bootstrapConfig({ ...routableFixtures, E2E_FIXTURE_PUBLIC_HOST: host })
    assert.equal(config.fixtures?.advertisedHost, host)
  }
})

test('remote bootstrap selects only the explicit disposable DB and HTTPS fixture settings', () => {
  const env = {
    ...routableFixtures,
    E2E_DATABASE: 'postgres',
    E2E_POSTGRES_TEST_URL: 'postgres://fixture:test-secret@127.0.0.1/disposable',
    CORAL_TEST_POSTGRES_URL: 'postgres://developer-secret@localhost/production',
    NODE_TLS_REJECT_UNAUTHORIZED: '0',
    SSL_CERT_FILE: '/developer/ca.pem',
    CORAL_WEB_AUTH_MODE: 'disabled',
  }
  const config = bootstrapConfig(env)
  assert.equal(config.postgresURL, env.E2E_POSTGRES_TEST_URL)
  assert.equal(config.database, 'postgres')
  assert.equal(config.suite, 'main')
  assert.deepEqual(config.fixtures, {
    bindHost: '0.0.0.0',
    advertisedHost: '11.0.0.1',
    tls: { certFile: '/fixture/cert.pem', keyFile: '/fixture/key.pem', caFile: '/fixture/ca.pem' },
  })
  assert.doesNotMatch(JSON.stringify(config.childEnv), /secret|postgres|developer|disabled/)
  assert.ok(!('NODE_TLS_REJECT_UNAUTHORIZED' in config.childEnv))
  assert.ok(!('SSL_CERT_FILE' in config.childEnv))
  assert.equal(
    bootstrapConfig({ ...routableFixtures, CORAL_TEST_POSTGRES_URL: env.CORAL_TEST_POSTGRES_URL })
      .postgresURL,
    undefined,
  )
  for (const [extra, message] of [
    [{ E2E_SUITE: 'desktop' }, /Only the main suite selects a database/],
    [{ E2E_SUITE: 'local-mode' }, /Only the main suite selects a database/],
    [{ E2E_HARNESS_PROBES: '1' }, /Harness probes run on SQLite/],
    [{ E2E_SUITE: 'multi-instance', E2E_HARNESS_PROBES: '1' }, /Harness probes run on SQLite/],
    [
      { E2E_BASE_URL: 'http://127.0.0.1:3000', E2E_ALLOW_ATTACHED: '1' },
      /Attached mode requires --suite local-mode/,
    ],
  ] as const) {
    assert.throws(() => bootstrapConfig({ ...env, ...extra }), message)
  }
  for (const url of [
    'not-a-url-with-secret',
    'https://secret@example.com/db',
    'postgres://localhost/',
  ]) {
    assert.throws(() => bootstrapConfig({ ...env, E2E_POSTGRES_TEST_URL: url }), {
      message: 'Reel requires a disposable Postgres database URL',
    })
  }
})

test('fixture bootstrap accepts HTTP private hostnames and requires complete TLS settings', () => {
  assert.equal(bootstrapConfig({ E2E_SUITE: 'local-mode' }).fixtures, undefined)
  assert.deepEqual(
    bootstrapConfig({
      E2E_FIXTURE_BIND_HOST: '0.0.0.0',
      E2E_FIXTURE_PUBLIC_HOST: 'sidecar',
    }).fixtures,
    { bindHost: '0.0.0.0', advertisedHost: 'sidecar' },
  )
  for (const key of Object.keys(routableFixtures)) {
    assert.throws(
      () => bootstrapConfig({ ...routableFixtures, [key]: '' }),
      /Fixture settings require/,
    )
  }
  for (const host of ['https://sidecar', 'sidecar:443', 'user@sidecar', 'sidecar/path']) {
    assert.throws(() => bootstrapConfig({ ...routableFixtures, E2E_FIXTURE_PUBLIC_HOST: host }))
  }
})

test('fixture TLS failures are safe and occur before a listener starts', async () => {
  const fixtures = bootstrapConfig(routableFixtures).fixtures!
  await assert.rejects(validateFixtureTLS(fixtures), {
    message:
      'Fixture TLS requires readable certificate/key/CA files and a valid certificate for the advertised host',
  })
  await validateFixtureTLS(loopbackFixtures)
})

test('upstream fixture uses explicit addresses and closes its listener', async () => {
  let finish: (() => unknown) | undefined
  const fixture = await listen(
    {
      onTestFinished: (callback) => {
        finish = () => callback({} as never)
      },
    },
    createServer((_request, response) => response.end('fixture')),
    loopbackFixtures,
  )
  assert.equal(new URL(fixture.origin).hostname, '127.0.0.1')
  try {
    assert.equal(await (await fetch(fixture.origin)).text(), 'fixture')
  } finally {
    await finish!()
  }
  await assert.rejects(fetch(fixture.origin, { signal: AbortSignal.timeout(1000) }))
})

test('attached targets require explicit disposable loopback consent', () => {
  for (const target of [
    'https://example.com',
    'http://localhost:3000',
    'http://127.0.0.1/path',
    'http://secret@127.0.0.1',
    'http://127.0.0.1/?token=secret',
  ]) {
    assert.throws(() => bootstrapConfig({ E2E_BASE_URL: target, E2E_ALLOW_ATTACHED: '1' }))
  }
  assert.throws(() => bootstrapConfig({ E2E_BASE_URL: 'http://127.0.0.1:3000' }))
  assert.equal(
    bootstrapConfig({
      E2E_SUITE: 'local-mode',
      E2E_BASE_URL: 'http://127.0.0.1:3000',
      E2E_ALLOW_ATTACHED: '1',
    }).attached,
    'http://127.0.0.1:3000',
  )
})

test('the desktop suite drives the package it builds, or an installed app it names', () => {
  const windowsProfile = {
    SYSTEMROOT: 'C:\\Windows',
    APPDATA: 'C:\\Users\\reel\\AppData\\Roaming',
    LOCALAPPDATA: 'C:\\Users\\reel\\AppData\\Local',
    USERPROFILE: 'C:\\Users\\reel',
    TEMP: 'C:\\Users\\reel\\AppData\\Local\\Temp',
    TMP: 'C:\\Users\\reel\\AppData\\Local\\Temp',
  }
  const env = {
    E2E_SUITE: 'desktop',
    PATH: '/bin',
    DISPLAY: ':99',
    CORAL_CONFIG_DIR: '/private',
    ...windowsProfile,
  }
  const packaged = bootstrapConfig(env)
  assert.equal(packaged.desktop?.executable, desktopExecutable)
  // Windows apps and tools find their system and profile folders through these.
  assert.deepEqual(packaged.desktop?.env, {
    LANG: 'C.UTF-8',
    PATH: '/bin',
    DISPLAY: ':99',
    ...windowsProfile,
  })
  for (const [key, value] of Object.entries(windowsProfile))
    assert.equal(packaged.childEnv[key as keyof typeof packaged.childEnv], value)
  assert.ok(!JSON.stringify(packaged.childEnv).includes('private'))

  const installed = bootstrapConfig({
    ...env,
    E2E_DESKTOP_EXECUTABLE: 'installed/Coral.exe',
  })
  assert.equal(installed.desktop?.executable, join(process.cwd(), 'installed', 'Coral.exe'))
  assert.equal(
    bootstrapConfig({ E2E_DESKTOP_EXECUTABLE: 'installed/Coral.exe' }).desktop,
    undefined,
  )
})

test('managed child configuration does not inherit developer credentials, state or exporters', () => {
  const config = bootstrapConfig({
    ...routableFixtures,
    PATH: '/bin',
    CORAL_CONFIG_DIR: '/private',
    CORAL_WEB_SESSION_SECRET: 'secret',
    CORAL_TEST_POSTGRES_URL: 'postgres://production',
    OTEL_EXPORTER_OTLP_ENDPOINT: 'https://example.com',
  })
  assert.equal(config.childEnv.PATH, '/bin')
  assert.ok(!JSON.stringify(config.childEnv).match(/private|secret|postgres|example/))
  assert.notEqual(config.runDir, bootstrapConfig(routableFixtures).runDir)
})

test('ffmpeg discovery distinguishes missing, available and broken optional converters', async () => {
  const root = await mkdtemp(join(tmpdir(), 'reel-ffmpeg-'))
  try {
    const missing = await detectFfmpeg(join(root, 'missing'))
    assert.equal(missing.binary, null)
    assert.match(missing.notice!, /ffmpeg not found; keeping WebM recordings/)

    const available = join(root, 'available')
    await writeFile(
      available,
      `#!${process.execPath}\nprocess.exit(process.argv[2] === '-version' ? 0 : 7)\n`,
      { mode: 0o700 },
    )
    assert.deepEqual(await detectFfmpeg(available), { binary: available, notice: undefined })

    const broken = join(root, 'broken')
    await writeFile(broken, `#!${process.execPath}\nprocess.exit(7)\n`, { mode: 0o700 })
    const failed = await detectFfmpeg(broken)
    assert.equal(failed.binary, null)
    assert.match(failed.notice!, /ffmpeg check failed/)
    assert.doesNotMatch(failed.notice!, /not found/)
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})

test('results viewer permits only loopback and an explicitly configured exact host', async () => {
  const root = await mkdtemp(join(tmpdir(), 'reel-hosts-'))
  const externalOrigin = 'https://reef.example.ts.net:9324'
  try {
    for (const configuredOrigin of [undefined, externalOrigin, `${externalOrigin}/`]) {
      const server = await resultsServer(root, 0, configuredOrigin)
      const address = server.address() as { address: string; port: number }
      assert.equal(address.address, '127.0.0.1')
      const localOrigin = `http://127.0.0.1:${address.port}`
      try {
        assert.equal((await fetch(localOrigin)).status, 200)
        for (const [host, expected] of [
          ['reef.example.ts.net:9324', configuredOrigin ? 200 : 403],
          ['other.example.ts.net:9324', 403],
          ['reef.example.ts.net', 403],
          ['reef.example.ts.net:9325', 403],
          ['reef.example.ts.net.attacker.example:9324', 403],
          ['attacker.example', 403],
        ] as const) {
          const status = await new Promise<number | undefined>((resolve, reject) => {
            get(
              localOrigin,
              { headers: { Host: host, 'X-Forwarded-Host': 'reef.example.ts.net:9324' } },
              (response) => {
                response.resume()
                resolve(response.statusCode)
              },
            ).on('error', reject)
          })
          assert.equal(status, expected, `${configuredOrigin ?? 'default'}: ${host}`)
        }
      } finally {
        server.closeAllConnections()
        await new Promise<void>((resolve, reject) =>
          server.close((error) => (error ? reject(error) : resolve())),
        )
      }
    }
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})

test('results viewer rejects malformed origins rather than widening access', async () => {
  const root = await mkdtemp(join(tmpdir(), 'reel-origins-'))
  try {
    for (const origin of [
      '',
      'reef.example.ts.net:9324',
      'ftp://reef.example.ts.net',
      'https://*.ts.net',
      'https://user:secret@reef.example.ts.net',
      'https://reef.example.ts.net/path',
      'https://reef.example.ts.net/?token=secret',
      'https://reef.example.ts.net/#fragment',
      'https://reef.example.ts.net,other.example.ts.net',
    ]) {
      await assert.rejects(async () => {
        const server = await resultsServer(root, 0, origin)
        await new Promise<void>((resolve, reject) =>
          server.close((error) => (error ? reject(error) : resolve())),
        )
      }, /Invalid E2E_RESULTS_ORIGIN/)
    }
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})

test('identity fixture controls require same-origin JSON and teardown closes the listener', async () => {
  const provider = await startIdentityProvider(
    'http://127.0.0.1:9/auth/oidc/callback',
    'http://127.0.0.1:9',
  )
  try {
    const endpoint = `${provider.origin}/fixture/accounts`
    const denied = await fetch(endpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ label: 'Alice' }),
    })
    assert.equal(denied.status, 403)
    const created = await fetch(endpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Origin: provider.origin },
      body: JSON.stringify({ label: 'Alice' }),
    })
    assert.equal(created.status, 200)
    const user = await created.json()
    assert.notEqual(user.subject, provider.owner.subject)
    assert.equal(user.label, 'Alice')
    const ownerRemoval = await fetch(`${endpoint}/${provider.owner.subject}`, {
      method: 'DELETE',
      headers: { Origin: provider.origin },
    })
    assert.equal(ownerRemoval.status, 403)
    const removed = await fetch(`${endpoint}/${user.subject}`, {
      method: 'DELETE',
      headers: { Origin: provider.origin },
    })
    assert.equal(removed.status, 204)
  } finally {
    await provider.close()
    await provider.close()
  }
  await assert.rejects(
    fetch(`${provider.origin}/.well-known/openid-configuration`, {
      signal: AbortSignal.timeout(1000),
    }),
  )
})

test('results server supports playback ranges but conceals files outside its root', async () => {
  const tmp = await mkdtemp(join(tmpdir(), 'reel-viewer-'))
  const root = join(tmp, 'runs')
  await mkdir(root)
  await writeFile(join(tmp, 'secret'), 'secret')
  await writeFile(join(root, 'session.webm'), '0123456789')
  await symlink(join(tmp, 'secret'), join(root, 'outside'))
  await mkdir(join(root, '<script>bad'))
  const server = await resultsServer(root)
  const origin = `http://127.0.0.1:${(server.address() as { port: number }).port}`
  try {
    const index = await fetch(origin)
    assert.match(await index.text(), /&lt;script&gt;bad/)
    for (const path of ['/outside', '/%2e%2e%2fsecret', '/missing']) {
      const response = await fetch(origin + path)
      assert.equal(response.status, 404)
      assert.equal(await response.text(), 'Not found')
    }
    const rebound = await new Promise<number | undefined>((resolve, reject) => {
      get(origin, { headers: { Host: 'attacker.example' } }, (response) => {
        response.resume()
        resolve(response.statusCode)
      }).on('error', reject)
    })
    assert.equal(rebound, 403)
    const range = await fetch(origin + '/session.webm', { headers: { Range: 'bytes=2-5' } })
    assert.equal(range.status, 206)
    assert.equal(range.headers.get('content-range'), 'bytes 2-5/10')
    assert.equal(await range.text(), '2345')
    const invalid = await fetch(origin + '/session.webm', { headers: { Range: 'bytes=30-' } })
    assert.equal(invalid.status, 416)
    const head = await fetch(origin + '/session.webm', { method: 'HEAD' })
    assert.equal(head.headers.get('content-length'), '10')
    assert.equal(await head.text(), '')
  } finally {
    server.closeAllConnections()
    await new Promise<void>((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve())),
    )
    await rm(tmp, { recursive: true, force: true })
  }
})

test('a parity entry snapshots the shared definition it runs, not harness support', async (context) => {
  const dir = await mkdtemp(join(tmpdir(), 'reel-source-'))
  context.after(() => rm(dir, { recursive: true, force: true }))
  const entry = join(reefRoot, 'e2e/scenarios/local-mode/parity/binary-output.test.ts')
  await snapshotSource(entry, join(dir, 'scenario.ts'))
  const snapshot = await readFile(join(dir, 'scenario.ts'), 'utf8')
  assert.ok(snapshot.startsWith(await readFile(entry, 'utf8')))
  assert.ok(snapshot.includes('// Shared definition: e2e/scenarios/main/activity/binary-output.ts'))
  assert.ok(snapshot.includes('export async function binaryOutput('))
  assert.ok(!snapshot.includes('export async function withOwner('))
})

test('a Coral that exits during startup shows how it ended and its last output', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'reel-exit-'))
  try {
    const coralBinary = join(dir, 'coral')
    // Coral's own report of a bind failure, on a port the harness never reserved:
    // without sign-in, Coral binds port zero.
    await writeFile(
      coralBinary,
      `#!/bin/sh\necho start >> "${dir}/starts"\necho "starting"\n` +
        'echo "Error: failed to bind MCP HTTP server to 127.0.0.1:4100" >&2\n' +
        'echo "    1: Address already in use (os error 98)" >&2\nexit 3\n',
      { mode: 0o755 },
    )
    await assert.rejects(
      startStack({ coralBinary, childEnv: { PATH: process.env.PATH }, runDir: dir }),
      (error: Error) => {
        assert.match(error.message, /exited \(3\) during startup/)
        assert.match(error.message, /failed to bind MCP HTTP server to 127\.0\.0\.1:4100/)
        return true
      },
    )
    // A port the harness did not reserve is not the harness's collision to retry.
    assert.equal(await readFile(join(dir, 'starts'), 'utf8'), 'start\n')
  } finally {
    await rm(dir, { recursive: true, force: true })
  }
})

/** Coral's report of an MCP listener it could not bind. */
const coralBindFailure = (address: string, cause: string) =>
  `Coral starting\nError: failed to start MCP HTTP server: failed to bind MCP HTTP server to ${address}\n\n` +
  `Caused by:\n    0: failed to bind MCP HTTP server to ${address}\n    1: ${cause}\n`

test('a final bind failure on a reserved port is a collision; nothing else is', () => {
  const inUse = 'Address already in use (os error 98)'
  const cases: [string, string, number | undefined][] = [
    ['Coral MCP', coralBindFailure('127.0.0.1:40537', inUse), 40537],
    [
      'Coral auth support',
      `Error: failed to bind auth-support listener to 127.0.0.1:40537\n\nCaused by:\n    ${inUse}`,
      40537,
    ],
    [
      'Run Executor health',
      `Error: bind the health listener on 127.0.0.1:40537\n\nCaused by:\n    ${inUse}\n`,
      40537,
    ],
    [
      'Reef',
      'WARNING: Coral web authentication is disabled\n' +
        'Coral web startup failed: HTTP listener: listen EADDRINUSE: address already in use 127.0.0.1:40537\n',
      40537,
    ],
    ['a port the harness did not reserve', coralBindFailure('127.0.0.1:4100', inUse), undefined],
    [
      'a longer port with a reserved prefix',
      coralBindFailure('127.0.0.1:405370', inUse),
      undefined,
    ],
    [
      'another bind failure',
      coralBindFailure('127.0.0.1:40537', 'Permission denied (os error 13)'),
      undefined,
    ],
    [
      'an address in use in an earlier report',
      coralBindFailure('127.0.0.1:40537', inUse) + 'Error: the database is unreachable\n',
      undefined,
    ],
    [
      'the port and the cause in different reports',
      `Error: bind 127.0.0.1:40537 later\n${coralBindFailure('127.0.0.1:4100', inUse)}`,
      undefined,
    ],
    ['no error report', `bind 127.0.0.1:40537: ${inUse}\n`, undefined],
  ]
  for (const [name, output, port] of cases)
    assert.equal(reservedPortTaken(output, [40537, 40538]), port, name)
})

test('a reserved port taken before Coral binds it starts a SQLite stack again on fresh ports, not a Postgres one', async (context) => {
  const cases = [
    // The second start fails for another reason, so no third follows.
    {
      database: 'sqlite',
      postgresURL: undefined,
      starts: 2,
      ending: /an unrelated startup failure/,
    },
    // Coral used the runner's one database before it bound the port.
    {
      database: 'postgres',
      postgresURL: 'postgresql://reel@127.0.0.1:1/reel',
      starts: 1,
      ending: /Address already in use/,
    },
  ]
  for (const { database, postgresURL, starts: expectedStarts, ending } of cases)
    await context.test(database, async (scenario) => {
      const dir = await mkdtemp(join(tmpdir(), 'reel-collision-'))
      scenario.after(() => rm(dir, { recursive: true, force: true }))
      const warn = scenario.mock.method(console, 'warn', () => {})
      // Stands in for Coral. Each start records the auth listener it was configured
      // with. The first asks the test to take that port, then binds it for real and
      // reports the bind failure as Coral does. The second fails for another reason.
      const coralBinary = join(dir, 'coral')
      await writeFile(
        coralBinary,
        `#!${process.execPath}
const { appendFileSync, existsSync, readFileSync, writeFileSync } = require('node:fs')
const { createServer } = require('node:net')
const { join } = require('node:path')
const dir = ${JSON.stringify(dir)}
const config = readFileSync(join(process.env.CORAL_CONFIG_DIR, 'config.toml'), 'utf8')
const [, port, publicURL] = config.match(/\\[listeners\\.auth\\]\\nbind = '127\\.0\\.0\\.1:(\\d+)'\\npublic_url = "([^"]+)"/)
appendFileSync(join(dir, 'starts'), port + ' ' + publicURL + '\\n')
if (readFileSync(join(dir, 'starts'), 'utf8').trim().split('\\n').length > 1) {
  console.error('Error: an unrelated startup failure')
  process.exit(3)
}
writeFileSync(join(dir, 'take'), port)
const wait = setInterval(() => {
  if (!existsSync(join(dir, 'taken'))) return
  clearInterval(wait)
  createServer()
    .once('error', (error) => {
      const address = '127.0.0.1:' + port
      console.error('Error: failed to bind auth-support listener to ' + address + '\\n\\nCaused by:')
      console.error('    0: failed to bind auth-support listener to ' + address)
      console.error('    1: ' + (error.code === 'EADDRINUSE' ? 'Address already in use (os error 98)' : error.message))
      process.exit(1)
    })
    .listen(Number(port), '127.0.0.1', () => process.exit(4))
}, 20)
`,
        { mode: 0o755 },
      )
      const started = startStack({
        coralBinary,
        childEnv: { PATH: process.env.PATH },
        runDir: dir,
        authenticated: true,
        postgresURL,
      }).then(
        () => assert.fail('the stack started'),
        (error: Error) => error,
      )
      let port: number | undefined
      const deadline = Date.now() + 10_000
      while (port === undefined) {
        try {
          port = Number(await readFile(join(dir, 'take'), 'utf8'))
        } catch {
          if (Date.now() > deadline)
            assert.fail(`The stand-in Coral never named its port: ${(await started).message}`)
          await new Promise((resolve) => setTimeout(resolve, 20))
        }
      }
      // Another process takes the port after the harness released it.
      const squatter = createServer()
      await new Promise<void>((resolve, reject) => {
        squatter.once('error', reject)
        squatter.listen(port, '127.0.0.1', resolve)
      })
      scenario.after(() => new Promise((resolve) => squatter.close(resolve)))
      await writeFile(join(dir, 'taken'), '')

      const error = await started
      assert.match(error.message, /during startup/)
      assert.match(error.message, ending)
      const starts = (await readFile(join(dir, 'starts'), 'utf8')).trim().split('\n')
      assert.equal(starts.length, expectedStarts)
      const [first, second] = starts.map((line) => line.split(' '))
      assert.deepEqual(first, [String(port), `http://127.0.0.1:${port}`])
      const retries = warn.mock.calls
        .map((call) => String(call.arguments[0]))
        .filter((message) => message.includes('Starting the stack again'))
      assert.equal(retries.length, expectedStarts - 1)
      if (!second) return
      assert.notEqual(second[0], first[0])
      // The issuer follows the fresh port.
      assert.equal(second[1], `http://127.0.0.1:${second[0]}`)
      assert.match(retries[0], new RegExp(`reserved port ${port}\\b`))
      assert.match(await readFile(join(dir, 'coral.log'), 'utf8'), /--- Reel: stack attempt 2 ---/)
    })
})
