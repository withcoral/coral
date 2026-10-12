import { generateKeyPairSync, createPrivateKey, randomBytes } from 'node:crypto'
import { execFileSync } from 'node:child_process'
import {
  existsSync,
  lstatSync,
  mkdirSync,
  readFileSync,
  readlinkSync,
  renameSync,
  symlinkSync,
  unlinkSync,
  writeFileSync,
} from 'node:fs'
import { dirname, join, relative, resolve } from 'node:path'

export const ports = Object.freeze({ web: 5173, auth: 9080, grpc: 14555, mcp: 14556 })
const marker = 'coral-development-v1\n'
const fields = {
  TAILNET_HOST: /^[a-z0-9][a-z0-9.-]*\.ts\.net$/,
  WORKOS_ENVIRONMENT_CLIENT_ID: /^client_[A-Z0-9]{26}$/,
  WORKOS_ORGANIZATION_ID: /^org_[A-Z0-9]{26}$/,
  WORKOS_CONNECT_CLIENT_ID: /^client_[A-Z0-9]{26}$/,
  WORKOS_OWNER_SUB: /^user_[A-Z0-9]{26}$/,
  WORKOS_CONNECT_CLIENT_SECRET: /^[A-Za-z0-9_.-]{16,512}$/,
}

export function validateField(name, value) {
  if (typeof value !== 'string' || !value || value !== value.trim())
    throw new Error(`Invalid or missing ${name}`)
  if (name === 'WORKOS_ISSUER') {
    let url
    try {
      url = new URL(value)
    } catch {
      throw new Error('WORKOS_ISSUER must be an HTTPS origin')
    }
    if (url.protocol !== 'https:' || url.origin !== value)
      throw new Error('WORKOS_ISSUER must be a canonical HTTPS origin without a trailing slash')
  } else if (!fields[name]?.test(value)) {
    throw new Error(`Invalid or missing ${name}`)
  }
  return value
}

export function parseEnv(text) {
  const values = {}
  for (const line of text.split(/\r?\n/)) {
    if (!line || line.startsWith('#')) continue
    const separator = line.indexOf('=')
    const name = line.slice(0, separator)
    if (separator < 1 || !/^[A-Z][A-Z0-9_]*$/.test(name) || Object.hasOwn(values, name))
      throw new Error('Invalid or duplicate field in development env file')
    values[name] = line.slice(separator + 1)
  }
  return values
}

function stat(path) {
  try {
    return lstatSync(path)
  } catch (error) {
    if (error.code === 'ENOENT') return null
    throw error
  }
}

function privateDirectory(path) {
  const info = stat(path)
  if (info && (!info.isDirectory() || info.isSymbolicLink() || info.mode & 0o077))
    throw new Error(`Expected a private directory: ${path}`)
  if (!info) mkdirSync(path, { mode: 0o700 })
}

function privateRead(path) {
  const info = stat(path)
  if (!info?.isFile() || info.isSymbolicLink() || info.mode & 0o077)
    throw new Error(`Expected a private regular file: ${path}`)
  return readFileSync(path)
}

function writePrivate(path, contents) {
  if (stat(path)) privateRead(path)
  const temporary = `${path}.${randomBytes(8).toString('hex')}.tmp`
  try {
    writeFileSync(temporary, contents, { flag: 'wx', mode: 0o600 })
    renameSync(temporary, path)
  } finally {
    if (existsSync(temporary)) unlinkSync(temporary)
  }
}

function checkWebEnv(root) {
  const path = join(root, 'apps/reef/.env')
  const info = stat(path)
  if (
    info &&
    (!info.isSymbolicLink() ||
      resolve(dirname(path), readlinkSync(path)) !== join(root, '.dev/web.env'))
  ) {
    throw new Error(
      'apps/reef/.env belongs to another setup. Move it aside yourself before continuing; it was not overwritten.',
    )
  }
  for (const name of ['.env.local', '.env.development', '.env.development.local']) {
    if (stat(join(root, 'apps/reef', name)))
      throw new Error(
        `apps/reef/${name} could override development configuration. Move it aside yourself first.`,
      )
  }
}

export function initialize(root) {
  checkWebEnv(root)
  try {
    execFileSync('git', ['check-ignore', '--quiet', '--no-index', '.dev/setup.env'], {
      cwd: root,
      stdio: 'ignore',
    })
  } catch {
    throw new Error('.dev/ must be Git-ignored before storing development credentials')
  }
  const directory = join(root, '.dev')
  if (stat(directory)) {
    privateDirectory(directory)
    if (privateRead(join(directory, 'setup-version')).toString() !== marker)
      throw new Error('.dev/ belongs to another setup; refusing to overwrite it')
  } else {
    privateDirectory(directory)
    writePrivate(join(directory, 'setup-version'), marker)
  }
  // The wizard's env upsert helper must never follow an existing symlink.
  if (stat(join(directory, 'setup.env'))) privateRead(join(directory, 'setup.env'))
  return directory
}

export function readInputs(root) {
  initialize(root)
  const input = parseEnv(privateRead(join(root, '.dev/setup.env')).toString())
  for (const name of ['WORKOS_ISSUER', ...Object.keys(fields)]) validateField(name, input[name])
  for (const name of Object.keys(input))
    if (name !== 'WORKOS_ISSUER' && !Object.hasOwn(fields, name))
      throw new Error(`Unknown development field: ${name}`)
  return input
}

export function urls(host) {
  validateField('TAILNET_HOST', host)
  return { web: `https://${host}`, auth: `https://${host}:8443`, mcp: `https://${host}:10000` }
}

export function configure(root) {
  const input = readInputs(root)
  const directory = join(root, '.dev')
  const publicInput = { ...input }
  delete publicInput.WORKOS_CONNECT_CLIENT_SECRET
  if (existsSync(join(directory, 'state/coral.db'))) {
    let previous
    try {
      previous = JSON.parse(privateRead(join(directory, 'settings.json')).toString())
    } catch {
      throw new Error(
        'Existing development state has no valid setup identity; refusing to overwrite it',
      )
    }
    for (const field of ['WORKOS_ISSUER', 'WORKOS_ORGANIZATION_ID', 'WORKOS_OWNER_SUB']) {
      if (previous[field] !== input[field])
        throw new Error(
          `Cannot change ${field} after the development database has been created; use a fresh checkout/setup`,
        )
    }
  }
  privateDirectory(join(directory, 'state'))
  privateDirectory(join(directory, 'secrets'))
  const signingKey = join(directory, 'secrets/session.pk8')
  if (!stat(signingKey)) {
    const { privateKey } = generateKeyPairSync('ec', {
      namedCurve: 'prime256v1',
      privateKeyEncoding: { type: 'pkcs8', format: 'der' },
      publicKeyEncoding: { type: 'spki', format: 'pem' },
    })
    writePrivate(signingKey, privateKey)
  }
  try {
    const key = createPrivateKey({ key: privateRead(signingKey), type: 'pkcs8', format: 'der' })
    if (key.asymmetricKeyType !== 'ec' || key.asymmetricKeyDetails.namedCurve !== 'prime256v1')
      throw new Error()
  } catch {
    throw new Error('Existing development signing key is invalid; it was not replaced')
  }
  const sessionFile = join(directory, 'secrets/web-session')
  if (!stat(sessionFile)) writePrivate(sessionFile, randomBytes(32).toString('hex'))
  const sessionSecret = privateRead(sessionFile).toString()
  if (!/^[a-f0-9]{64}$/.test(sessionSecret))
    throw new Error('Existing web session secret is invalid; it was not replaced')
  const publicUrls = urls(input.TAILNET_HOST)
  const webEnv = {
    CORAL_WEB_AUTH_MODE: 'required',
    CORAL_WEB_PUBLIC_URL: publicUrls.web,
    CORAL_WEB_AUTH_ISSUER: publicUrls.auth,
    CORAL_WEB_SESSION_SECRET: sessionSecret,
    CORAL_ENDPOINT: `http://127.0.0.1:${ports.grpc}`,
  }
  const q = JSON.stringify // All values are validated single-line strings; JSON quoting is TOML-compatible here.
  const config = `[listeners.grpc]\nbind = "127.0.0.1:${ports.grpc}"\n
[listeners.mcp]\nbind = "127.0.0.1:${ports.mcp}"\npublic_url = ${q(publicUrls.mcp)}\n
[listeners.auth]\nbind = "127.0.0.1:${ports.auth}"\npublic_url = ${q(publicUrls.auth)}\n
[listeners.web]\npublic_url = ${q(publicUrls.web)}\n
[auth.session]\nsigning_key_file = "../secrets/session.pk8"\n
[auth.provider]\nissuer = ${q(input.WORKOS_ISSUER)}\nclient_id = ${q(input.WORKOS_CONNECT_CLIENT_ID)}\nclient_secret_env = "WORKOS_CONNECT_CLIENT_SECRET"\nbootstrap_owner_subject = ${q(input.WORKOS_OWNER_SUB)}\n
[auth.provider.required_claims]\nemail_verified = true\n
[auth.provider.access_token]\naudience = ${q(input.WORKOS_ENVIRONMENT_CLIENT_ID)}\n
[auth.provider.access_token.required_claims]\norg_id = ${q(input.WORKOS_ORGANIZATION_ID)}\n`
  writePrivate(join(directory, 'state/config.toml'), config)
  writePrivate(
    join(directory, 'web.env'),
    Object.entries(webEnv)
      .map(([key, value]) => `${key}=${value}\n`)
      .join(''),
  )
  writePrivate(join(directory, 'settings.json'), JSON.stringify(publicInput, null, 2) + '\n')
  const envPath = join(root, 'apps/reef/.env')
  if (!stat(envPath)) symlinkSync(relative(dirname(envPath), join(directory, 'web.env')), envPath)
  return publicUrls
}

export function load(root) {
  if (!stat(join(root, '.dev/settings.json')))
    throw new Error('Run ./dev setup before starting development servers')
  const input = readInputs(root)
  let applied
  try {
    applied = JSON.parse(privateRead(join(root, '.dev/settings.json')).toString())
  } catch {
    throw new Error('Run ./dev setup before starting development servers')
  }
  const publicInput = { ...input }
  delete publicInput.WORKOS_CONNECT_CLIENT_SECRET
  if (
    !applied ||
    typeof applied !== 'object' ||
    Object.keys(applied).length !== Object.keys(publicInput).length ||
    Object.entries(publicInput).some(([name, value]) => applied[name] !== value)
  ) {
    throw new Error('Development settings changed; run ./dev setup again')
  }
  privateRead(join(root, '.dev/state/config.toml'))
  return {
    input,
    webEnv: parseEnv(privateRead(join(root, '.dev/web.env')).toString()),
    urls: urls(input.TAILNET_HOST),
  }
}

export function proxyPlan(host, status = {}) {
  validateField('TAILNET_HOST', host)
  return [
    [443, ports.web],
    [8443, ports.auth],
    [10000, ports.mcp],
  ].map(([port, target]) => {
    const authority = `${host}:${port}`
    const proxy = `http://127.0.0.1:${target}`
    const tcp = status.TCP?.[port]
    const web = status.Web?.[authority]
    const otherHost = Object.keys(status.Web ?? {}).some(
      (key) => key.endsWith(`:${port}`) && key !== authority,
    )
    if (
      (status.AllowFunnel?.[authority] && !web) ||
      otherHost ||
      (tcp && (tcp.HTTPS !== true || Object.keys(tcp).length !== 1)) ||
      (web &&
        (Object.keys(web.Handlers ?? {}).length !== 1 || web.Handlers?.['/']?.Proxy !== proxy)) ||
      Boolean(tcp) !== Boolean(web)
    ) {
      throw new Error(
        `Tailscale port ${port} has conflicting or incomplete configuration; it was not overwritten`,
      )
    }
    return {
      authority,
      proxy,
      existing: Boolean(web),
      funnel: Boolean(status.AllowFunnel?.[authority]),
      args: ['serve', '--bg', `--https=${port}`, proxy],
    }
  })
}

export function shellQuote(value) {
  return `'${value.replaceAll("'", "'\\''")}'`
}
