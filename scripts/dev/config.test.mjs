import assert from 'node:assert/strict'
import { execFileSync } from 'node:child_process'
import { createPrivateKey } from 'node:crypto'
import {
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
    web: 'https://developer.example.ts.net',
    auth: 'https://developer.example.ts.net:8443',
    mcp: 'https://developer.example.ts.net:10000',
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
  assert.match(config, /public_url = "https:\/\/developer\.example\.ts\.net:10000"/)
  assert.equal(load(root).urls.mcp, 'https://developer.example.ts.net:10000')
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

test('proxy planning preserves exact Serve/Funnel mappings and creates only private mappings', () => {
  const host = input.TAILNET_HOST
  const fresh = proxyPlan(host, {})
  assert.deepEqual(
    fresh.map((m) => m.args),
    [
      ['serve', '--bg', '--https=443', 'http://127.0.0.1:5173'],
      ['serve', '--bg', '--https=8443', 'http://127.0.0.1:9080'],
      ['serve', '--bg', '--https=10000', 'http://127.0.0.1:14556'],
    ],
  )
  const configured = { TCP: {}, Web: {} }
  for (const mapping of fresh) {
    configured.TCP[mapping.authority.split(':')[1]] = { HTTPS: true }
    configured.Web[mapping.authority] = { Handlers: { '/': { Proxy: mapping.proxy } } }
  }
  assert.ok(proxyPlan(host, configured).every((m) => m.existing && !m.funnel))
  // Every mix of private and public mappings is reused without changing visibility.
  for (let mask = 0; mask < 8; mask++) {
    const status = structuredClone(configured)
    status.AllowFunnel = Object.fromEntries(
      fresh.map((m, i) => [m.authority, Boolean(mask & (1 << i))]),
    )
    const snapshot = JSON.stringify(status)
    const plan = proxyPlan(host, status)
    assert.ok(plan.every((m) => m.existing))
    assert.deepEqual(plan.map((m) => m.funnel), fresh.map((_, i) => Boolean(mask & (1 << i))))
    assert.equal(JSON.stringify(status), snapshot)
  }
  const partial = structuredClone(configured)
  partial.AllowFunnel = { [`${host}:443`]: true }
  delete partial.TCP['10000']
  delete partial.Web[`${host}:10000`]
  const missing = proxyPlan(host, partial).filter((m) => !m.existing)
  assert.deepEqual(missing.map((m) => m.args), [fresh[2].args])
  assert.ok(missing.every((m) => !m.funnel))

  for (const mutate of [
    (s) => {
      s.Web[`${host}:443`].Handlers['/'].Proxy = 'http://127.0.0.1:9999'
    },
    (s) => {
      s.Web[`${host}:443`].Handlers['/oauth'] = { Proxy: 'http://127.0.0.1:9999' }
    },
    (s) => {
      s.AllowFunnel = { [`${host}:8443`]: true }
      delete s.Web[`${host}:8443`]
      delete s.TCP['8443']
    },
    (s) => {
      s.AllowFunnel = { [`${host}:10000`]: true }
      s.Web[`${host}:10000`].Handlers['/'].Proxy = 'http://127.0.0.1:9999'
    },
    (s) => {
      delete s.TCP['443']
    },
    (s) => {
      s.TCP['443'] = { TCPForward: '127.0.0.1:9999' }
    },
    (s) => {
      s.Web['another.example.ts.net:443'] = s.Web[`${host}:443`]
    },
  ]) {
    const status = structuredClone(configured)
    mutate(status)
    const snapshot = JSON.stringify(status)
    assert.throws(() => proxyPlan(host, status), /not overwritten/)
    assert.equal(JSON.stringify(status), snapshot)
  }
})
