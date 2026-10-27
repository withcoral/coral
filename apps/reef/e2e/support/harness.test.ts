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
