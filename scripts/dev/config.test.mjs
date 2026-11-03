import assert from 'node:assert/strict'
import { execFileSync } from 'node:child_process'
import { createPrivateKey } from 'node:crypto'
import {
  appendFileSync,
  existsSync,
  lstatSync,
  mkdtempSync,
  mkdirSync,
  readFileSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import test from 'node:test'
import {
  configure,
  initialize,
  load,
  parseEnv,
  proxyPlan,
  shellQuote,
  validateField,
} from './config.mjs'

const input = {
  TAILNET_HOST: 'developer.example.ts.net',
  WORKOS_ISSUER: 'https://auth.example.test',
  WORKOS_ENVIRONMENT_CLIENT_ID: 'client_' + '0'.repeat(26),
  WORKOS_ORGANIZATION_ID: 'org_' + '1'.repeat(26),
  WORKOS_CONNECT_CLIENT_ID: 'client_' + '2'.repeat(26),
  WORKOS_OWNER_SUB: 'user_' + '3'.repeat(26),
  WORKOS_CONNECT_CLIENT_SECRET: 'dummy-filesystem-only-secret-0123456789',
}

function fixture(t) {
  const root = mkdtempSync(join(tmpdir(), 'coral dev test '))
  t.after(() => rmSync(root, { recursive: true, force: true }))
  mkdirSync(join(root, 'apps/reef'), { recursive: true })
  writeFileSync(join(root, '.gitignore'), '/.dev/\n')
  execFileSync('git', ['init', '--quiet'], { cwd: root })
  return root
}
function saveInputs(root, values = input) {
  writeFileSync(
    join(root, '.dev/setup.env'),
    Object.entries(values)
      .map(([k, v]) => `${k}=${v}\n`)
      .join(''),
    { mode: 0o600 },
  )
}

test('setup creates portable private state and preserves keys, credentials and data on rerun', (t) => {
  const root = fixture(t)
  initialize(root)
  saveInputs(root)
  assert.deepEqual(configure(root), {
    web: 'https://developer.example.ts.net:8443',
    auth: 'https://developer.example.ts.net',
    mcp: 'https://developer.example.ts.net',
  })
  const keyPath = join(root, '.dev/secrets/session.pk8')
  const key = readFileSync(keyPath)
  assert.equal(
    createPrivateKey({ key, type: 'pkcs8', format: 'der' }).asymmetricKeyDetails.namedCurve,
    'prime256v1',
  )
  const web = readFileSync(join(root, '.dev/web.env'), 'utf8')
  const webEnv = parseEnv(web)
  assert.equal(webEnv.CORAL_WEB_AUTH_MODE, 'required')
  assert.equal(webEnv.CORAL_ENDPOINT, 'http://127.0.0.1:14555')
  assert.equal(webEnv.CORAL_WEB_SESSION_SECRET.length, 64)
  const config = readFileSync(join(root, '.dev/state/config.toml'), 'utf8')
  assert.match(config, /\[listeners.web\]/)
  assert.match(config, /\[listeners.auth\]/)
  assert.match(
    config,
    /\[listeners.mcp\]\nbind = "127.0.0.1:14556"\npublic_url = "https:\/\/developer\.example\.ts\.net"\n/,
  )
  assert.match(
    config,
    /\[listeners.auth\]\nbind = "127.0.0.1:9080"\npublic_url = "https:\/\/developer\.example\.ts\.net"\n/,
  )
  assert.equal(webEnv.CORAL_WEB_PUBLIC_URL, 'https://developer.example.ts.net:8443')
  assert.equal(webEnv.CORAL_WEB_AUTH_ISSUER, 'https://developer.example.ts.net')
  assert.equal(load(root).urls.mcp, 'https://developer.example.ts.net')
  assert.match(config, /signing_key_file = "\.\.\/secrets\/session.pk8"/)
  assert.ok(
    !config.includes('access_token_ttl_seconds'),
    'dev config leaves the access token TTL at the server default (30 days)',
  )
  assert.match(config, /\[auth.provider.access_token.required_claims\]/)
  assert.ok(config.includes(input.WORKOS_ORGANIZATION_ID))
  assert.ok(!config.includes(root), 'TOML does not bake in the checkout path')
  assert.ok(!config.includes(input.WORKOS_CONNECT_CLIENT_SECRET))
  assert.ok(
    !readFileSync(join(root, '.dev/settings.json'), 'utf8').includes(
      input.WORKOS_CONNECT_CLIENT_SECRET,
    ),
  )
  assert.ok(lstatSync(join(root, 'apps/reef/.env')).isSymbolicLink())
  writeFileSync(join(root, '.dev/state/coral.db'), 'existing-database')
  configure(root)
  assert.deepEqual(readFileSync(keyPath), key)
  assert.equal(readFileSync(join(root, '.dev/web.env'), 'utf8'), web)
  assert.equal(readFileSync(join(root, '.dev/state/coral.db'), 'utf8'), 'existing-database')
  assert.equal(load(root).input.WORKOS_CONNECT_CLIENT_SECRET, input.WORKOS_CONNECT_CLIENT_SECRET)
  for (const file of [
    'setup.env',
    'web.env',
    'settings.json',
    'state/config.toml',
    'secrets/session.pk8',
    'secrets/web-session',
  ]) {
    assert.equal(lstatSync(join(root, '.dev', file)).mode & 0o077, 0, file)
  }
})

test('run refuses configuration generated for the earlier per-port layout', (t) => {
  const root = fixture(t)
  initialize(root)
  saveInputs(root)
  configure(root)
  const webEnvPath = join(root, '.dev/web.env')
  const current = readFileSync(webEnvPath, 'utf8')
  writeFileSync(
    webEnvPath,
    current.replace(
      'CORAL_WEB_PUBLIC_URL=https://developer.example.ts.net:8443',
      'CORAL_WEB_PUBLIC_URL=https://developer.example.ts.net',
    ),
  )
  assert.throws(() => load(root), /Development URLs changed; run \.\/dev setup again/)
  configure(root)
  assert.equal(readFileSync(webEnvPath, 'utf8'), current)
  assert.equal(load(root).urls.web, 'https://developer.example.ts.net:8443')
})

test('Reef exports traces where Coral exports them', (t) => {
  const root = fixture(t)
  initialize(root)
  saveInputs(root)
  configure(root)
  assert.deepEqual(load(root).otelEnv, {})

  appendFileSync(
    join(root, '.dev/state/config.toml'),
    '\n[otel]\nendpoint = "http://127.0.0.1:4318"\nheaders = "authorization=key"\n\n[later]\nendpoint = "no"\n',
  )
  assert.deepEqual(load(root).otelEnv, {
    OTEL_EXPORTER_OTLP_ENDPOINT: 'http://127.0.0.1:4318',
    OTEL_EXPORTER_OTLP_HEADERS: 'authorization=key',
  })
})

test('Reef reads both TOML string forms from Coral trace settings', (t) => {
  const root = fixture(t)
  initialize(root)
  saveInputs(root)
  configure(root)

  appendFileSync(
    join(root, '.dev/state/config.toml'),
    "\n[otel]\nendpoint = 'http://127.0.0.1:4318'\nheaders = 'authorization=key'\n",
  )
  assert.deepEqual(load(root).otelEnv, {
    OTEL_EXPORTER_OTLP_ENDPOINT: 'http://127.0.0.1:4318',
    OTEL_EXPORTER_OTLP_HEADERS: 'authorization=key',
  })
})

test('run without setup has no filesystem side effects', (t) => {
  const root = fixture(t)
  assert.throws(() => load(root), /Run \.\/dev setup/)
  assert.ok(!existsSync(join(root, '.dev')))
})

test('setup never overwrites unrelated env files or adopts another .dev directory', (t) => {
  const root = fixture(t)
  const env = join(root, 'apps/reef/.env')
  writeFileSync(env, 'existing-private-configuration', { mode: 0o600 })
  assert.throws(() => initialize(root), /belongs to another setup/)
  assert.equal(readFileSync(env, 'utf8'), 'existing-private-configuration')
  assert.ok(!existsSync(join(root, '.dev')))
  rmSync(env)
  mkdirSync(join(root, '.dev'), { mode: 0o700 })
  writeFileSync(join(root, '.dev/other'), 'leave me alone')
  assert.throws(() => initialize(root), /private regular file/)
  assert.equal(readFileSync(join(root, '.dev/other'), 'utf8'), 'leave me alone')
})

test('setup rejects symlink escapes and requires a Git ignore rule', (t) => {
  const root = fixture(t)
  writeFileSync(join(root, '.gitignore'), '')
  assert.throws(() => initialize(root), /Git-ignored/)
  assert.ok(!existsSync(join(root, '.dev')))
  writeFileSync(join(root, '.gitignore'), '/.dev/\n')
  initialize(root)
  const outside = join(root, 'outside-secret')
  writeFileSync(outside, 'untouched', { mode: 0o600 })
  symlinkSync(outside, join(root, '.dev/setup.env'))
  assert.throws(() => initialize(root), /private regular file/)
  assert.equal(readFileSync(outside, 'utf8'), 'untouched')
})

test('existing state cannot silently change its identity provider, Organization or Owner', (t) => {
  const root = fixture(t)
  initialize(root)
  saveInputs(root)
  configure(root)
  writeFileSync(join(root, '.dev/state/coral.db'), 'existing')
  const before = readFileSync(join(root, '.dev/state/config.toml'), 'utf8')
  for (const [name, value] of [
    ['WORKOS_ISSUER', 'https://other.example.test'],
    ['WORKOS_ORGANIZATION_ID', 'org_' + '4'.repeat(26)],
    ['WORKOS_OWNER_SUB', 'user_' + '5'.repeat(26)],
  ]) {
    saveInputs(root, { ...input, [name]: value })
    assert.throws(() => configure(root), /Cannot change/)
    assert.equal(readFileSync(join(root, '.dev/state/config.toml'), 'utf8'), before)
  }
})

test('input validation and env parsing never evaluate shell text or echo secrets', () => {
  const value = '$(touch /tmp/must-not-run)'
  for (const name of Object.keys(input)) {
    assert.throws(
      () => validateField(name, value),
      (error) => !error.message.includes(value),
    )
  }
  assert.throws(() => parseEnv('KEY=one\nKEY=canary-secret'), /duplicate/)
  assert.equal(parseEnv(`KEY=${value}`).KEY, value)
  assert.equal(
    execFileSync('sh', ['-c', `printf %s ${shellQuote("path with ' quotes")}`], {
      encoding: 'utf8',
    }),
    "path with ' quotes",
  )
})

const host = input.TAILNET_HOST
const auth = (path) => [path, `http://127.0.0.1:9080${path}`]
const shared = Object.fromEntries([
  ['/', 'http://127.0.0.1:14556'],
  auth('/.well-known/oauth-authorization-server'),
  auth('/oauth'),
  auth('/auth/oidc'),
])
function serveStatus(byPort, funnel = []) {
  const status = { TCP: {}, Web: {}, AllowFunnel: {} }
  for (const [port, handlers] of Object.entries(byPort)) {
    status.TCP[port] = { HTTPS: true }
    status.Web[`${host}:${port}`] = {
      Handlers: Object.fromEntries(
        Object.entries(handlers).map(([path, proxy]) => [path, { Proxy: proxy }]),
      ),
    }
  }
  for (const port of funnel) status.AllowFunnel[`${host}:${port}`] = true
  return status
}
const current = { 443: shared, 8443: { '/': 'http://127.0.0.1:5173' } }
const legacy = {
  443: { '/': 'http://127.0.0.1:5173' },
  8443: { '/': 'http://127.0.0.1:9080' },
  10000: { '/': 'http://127.0.0.1:14556' },
}
const sharedArgs = (verb) => [
  [verb, '--bg', '--https=443', 'http://127.0.0.1:14556'],
  ...['/.well-known/oauth-authorization-server', '/oauth', '/auth/oidc'].map((path) => [
    verb,
    '--bg',
    '--https=443',
    `--set-path=${path}`,
    `http://127.0.0.1:9080${path}`,
  ]),
]
const reefArgs = (verb) => [[verb, '--bg', '--https=8443', 'http://127.0.0.1:5173']]
const summary = (plan) =>
  plan.map(({ authority, state, funnel, commands }) => ({ authority, state, funnel, commands }))

test('proxy planning reuses, completes or replaces only the mappings this setup owns', () => {
  const partialShared = { ...shared }
  delete partialShared['/oauth']
  for (const [name, status, expected] of [
    [
      'fresh machine gets private Serve',
      {},
      [
        {
          authority: `${host}:443`,
          state: 'missing',
          funnel: false,
          commands: sharedArgs('serve'),
        },
        { authority: `${host}:8443`, state: 'missing', funnel: false, commands: reefArgs('serve') },
      ],
    ],
    ...[[], [443], [8443], [443, 8443]].map((funnel) => [
      `current layout is reused with Funnel on [${funnel}]`,
      serveStatus(current, funnel),
      [443, 8443].map((port) => ({
        authority: `${host}:${port}`,
        state: 'ready',
        funnel: funnel.includes(port),
        commands: [],
      })),
    ]),
    ...[[], [443]].map((funnel) => [
      `a missing path is added without changing Funnel [${funnel}]`,
      serveStatus({ ...current, 443: partialShared }, funnel),
      [
        {
          authority: `${host}:443`,
          state: 'missing',
          funnel: funnel.includes(443),
          commands: [
            [
              funnel.includes(443) ? 'funnel' : 'serve',
              '--bg',
              '--https=443',
              '--set-path=/oauth',
              'http://127.0.0.1:9080/oauth',
            ],
          ],
        },
        { authority: `${host}:8443`, state: 'ready', funnel: false, commands: [] },
      ],
    ]),
    [
      'earlier per-port layout is replaced with private Serve, even from Funnel',
      serveStatus(legacy, [443, 8443, 10000]),
      [
        {
          authority: `${host}:443`,
          state: 'legacy',
          funnel: true,
          commands: [['serve', '--https=443', 'off'], ...sharedArgs('serve')],
        },
        {
          authority: `${host}:8443`,
          state: 'legacy',
          funnel: true,
          commands: [['serve', '--https=8443', 'off'], ...reefArgs('serve')],
        },
        {
          authority: `${host}:10000`,
          state: 'legacy',
          funnel: true,
          commands: [['serve', '--https=10000', 'off']],
        },
      ],
    ],
    [
      'port 10000 is ignored unless it holds the earlier MCP mapping',
      serveStatus({ ...current, 10000: { '/': 'http://127.0.0.1:9999' } }, [10000]),
      [443, 8443].map((port) => ({
        authority: `${host}:${port}`,
        state: 'ready',
        funnel: false,
        commands: [],
      })),
    ],
  ]) {
    const snapshot = JSON.stringify(status)
    assert.deepEqual(summary(proxyPlan(host, status)), expected, name)
    assert.equal(JSON.stringify(status), snapshot, name)
  }
})

test('proxy planning refuses mappings it does not own and changes nothing', () => {
  for (const [name, mutate] of [
    [
      '443 path to another service',
      (s) => (s.Web[`${host}:443`].Handlers['/oauth'].Proxy = 'http://127.0.0.1:9999'),
    ],
    [
      '443 extra path',
      (s) => (s.Web[`${host}:443`].Handlers['/auth'] = { Proxy: 'http://127.0.0.1:9080/auth' }),
    ],
    [
      'Reef port to another service',
      (s) => (s.Web[`${host}:8443`].Handlers['/'].Proxy = 'http://127.0.0.1:9999'),
    ],
    [
      'earlier auth mapping with extra handler settings',
      (s) =>
        (s.Web[`${host}:8443`].Handlers['/'] = {
          Proxy: 'http://127.0.0.1:9080',
          AcceptAppCaps: ['example.com/cap/dev'],
        }),
    ],
    [
      'earlier Reef mapping with an extra path',
      (s) =>
        (s.Web[`${host}:443`].Handlers = {
          '/': { Proxy: 'http://127.0.0.1:5173' },
          '/oauth': { Proxy: 'http://127.0.0.1:9080/oauth' },
        }),
    ],
    [
      'Funnel without a handler',
      (s) => {
        delete s.Web[`${host}:8443`]
        delete s.TCP['8443']
        s.AllowFunnel[`${host}:8443`] = true
      },
    ],
    ['TCP without a handler', (s) => delete s.Web[`${host}:443`]],
    ['handler without TCP', (s) => delete s.TCP['443']],
    ['TCP forwarding', (s) => (s.TCP['443'] = { TCPForward: '127.0.0.1:9999' })],
    ['another hostname', (s) => (s.Web['another.example.ts.net:443'] = s.Web[`${host}:443`])],
  ]) {
    const status = serveStatus(current)
    mutate(status)
    const snapshot = JSON.stringify(status)
    assert.throws(() => proxyPlan(host, status), /not overwritten/, name)
    assert.equal(JSON.stringify(status), snapshot, name)
  }
})
